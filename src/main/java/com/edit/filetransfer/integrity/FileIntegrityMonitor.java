package com.nithish.filetransfer.integrity;

import com.nithish.filetransfer.ChecksumUtils;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.*;

/**
 * Watches a directory tree for create/modify/delete and compares against
 * a persisted baseline. Two ideas make this behave sanely rather than
 * generating noise:
 *
 *  1. DEBOUNCE: a single logical file save can fire several raw
 *     filesystem events (editors do this constantly, and so does our
 *     own chunked transfer writer -- see markTransferActive below). A
 *     rapid-fire MODIFY for the same path resets a short timer instead
 *     of triggering an immediate check, so we evaluate once per burst,
 *     not once per event.
 *
 *  2. KNOWN TRANSFER ACTIVITY: TransferServer legitimately writes to a
 *     destination file many times during one transfer. Without an
 *     explicit signal, every one of those writes would look identical
 *     to file tampering. markTransferActive(path)/markTransferComplete()
 *     are how TransferServer tells FIM "trust me, I know about this
 *     one" -- while a path is marked active, FIM suppresses checks on
 *     it entirely. The baseline is updated explicitly by
 *     markTransferComplete() using the transfer's own verified SHA-256,
 *     not by re-deriving it from a filesystem event.
 *
 * A baseline is otherwise never auto-updated on a detected modification
 * -- an unexplained hash change stays flagged until something explicit
 * (a verified transfer, or a fresh createBaseline()) says otherwise.
 * Silently trusting every change would defeat the point of the feature.
 */
public final class FileIntegrityMonitor {

    private static final long DEBOUNCE_MILLIS = 250;

    private final Path root;
    private final BaselineManager baseline;
    private final IntegrityEventStore eventStore;
    private final Set<String> excludedFileNames;

    private WatchService watchService;
    private final Map<WatchKey, Path> watchKeys = new ConcurrentHashMap<>();
    private final Set<Path> activeTransferPaths = ConcurrentHashMap.newKeySet();
    private final Map<Path, ScheduledFuture<?>> pendingChecks = new ConcurrentHashMap<>();
    private ScheduledExecutorService debounceExecutor;
    private Thread watchThread;
    private volatile boolean running;

    private FileIntegrityMonitor(Path root, BaselineManager baseline, IntegrityEventStore eventStore, Set<String> excludedFileNames) {
        this.root = root.toAbsolutePath().normalize();
        this.baseline = baseline;
        this.eventStore = eventStore;
        this.excludedFileNames = excludedFileNames;
    }

    /** Explicit initial baseline: whatever's on disk right now becomes trusted. */
    public static FileIntegrityMonitor withFreshBaseline(Path root, Path baselineFile, Path eventLogFile,
                                                           Set<String> excludedFileNames) throws IOException {
        BaselineManager baseline = BaselineManager.createBaseline(root, baselineFile, excludedFileNames);
        return new FileIntegrityMonitor(root, baseline, new IntegrityEventStore(eventLogFile), excludedFileNames);
    }

    /** Loads whatever baseline already exists (empty if none) -- the normal restart path. */
    public static FileIntegrityMonitor withExistingBaseline(Path root, Path baselineFile, Path eventLogFile,
                                                              Set<String> excludedFileNames) throws IOException {
        BaselineManager baseline = BaselineManager.load(baselineFile);
        return new FileIntegrityMonitor(root, baseline, new IntegrityEventStore(eventLogFile), excludedFileNames);
    }

    public void start() throws IOException {
        watchService = FileSystems.getDefault().newWatchService();
        Files.createDirectories(root);
        registerAll(root);

        reconcile(); // catch anything that changed while nobody was watching

        running = true;
        debounceExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fim-debounce");
            t.setDaemon(true);
            return t;
        });
        watchThread = new Thread(this::watchLoop, "fim-watch");
        watchThread.setDaemon(true);
        watchThread.start();
        System.out.println("FIM started: monitoring " + root);
    }

    public void stop() throws IOException {
        running = false;
        if (watchService != null) {
            watchService.close(); // unblocks watchLoop's take() with ClosedWatchServiceException
        }
        if (debounceExecutor != null) {
            debounceExecutor.shutdownNow();
        }
        if (watchThread != null) {
            try { watchThread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        System.out.println("FIM stopped");
    }

    /** Called by TransferServer right before it starts writing a destination file for a new transfer. */
    public void markTransferActive(Path path) {
        activeTransferPaths.add(normalize(path));
        System.out.println("Known transfer activity: " + path + " (FIM checks suppressed until complete)");
    }

    /** Called by TransferServer once a transfer's whole-file checksum has been verified. */
    public void markTransferComplete(Path path, String verifiedSha256, long size) throws IOException {
        activeTransferPaths.remove(normalize(path));
        baseline.update(path, verifiedSha256, size);
        System.out.println("Baseline updated after verified transfer: " + path);
    }

    /** Called when a transfer ends WITHOUT a verified checksum (failed/incomplete) -- stop suppressing
     *  checks on this path, but do not touch the baseline with an unverified hash. */
    public void unmarkTransferActive(Path path) {
        activeTransferPaths.remove(normalize(path));
    }

    public BaselineManager getBaseline() {
        return baseline;
    }

    public IntegrityEventStore getEventStore() {
        return eventStore;
    }

    // --- reconciliation: catch drift that happened while the monitor wasn't running ---

    private void reconcile() throws IOException {
        Map<Path, BaselineManager.Entry> known = baseline.snapshot();
        Set<Path> seenOnDisk = new HashSet<>();

        if (Files.exists(root)) {
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path file : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
                    if (excludedFileNames.contains(file.getFileName().toString())) continue;
                    Path key = normalize(file);
                    seenOnDisk.add(key);

                    String currentHash;
                    try {
                        currentHash = ChecksumUtils.sha256Hex(file);
                    } catch (IOException e) {
                        continue; // file disappeared/locked mid-scan -- skip this cycle
                    }
                    BaselineManager.Entry entry = known.get(key);
                    if (entry == null) {
                        recordEvent(IntegrityEventType.FILE_CREATED, key, null, currentHash,
                                "detected during startup reconciliation");
                        baseline.update(key, currentHash, Files.size(file));
                    } else if (!entry.sha256.equals(currentHash)) {
                        recordEvent(IntegrityEventType.FILE_MODIFIED, key, entry.sha256, currentHash,
                                "detected during startup reconciliation");
                    }
                }
            }
        }

        for (Path key : known.keySet()) {
            if (!seenOnDisk.contains(key)) {
                recordEvent(IntegrityEventType.FILE_DELETED, key, known.get(key).sha256, null,
                        "detected during startup reconciliation");
                baseline.remove(key);
            }
        }
    }

    // --- live watching ---

    private void registerAll(Path start) throws IOException {
        try (Stream<Path> walk = Files.walk(start)) {
            for (Path dir : (Iterable<Path>) walk.filter(Files::isDirectory)::iterator) {
                WatchKey key = dir.register(watchService, ENTRY_CREATE, ENTRY_MODIFY, ENTRY_DELETE);
                watchKeys.put(key, dir);
            }
        }
    }

    private void watchLoop() {
        while (running) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (ClosedWatchServiceException | InterruptedException e) {
                break; // stop() was called
            }

            Path dir = watchKeys.get(key);
            if (dir == null) {
                key.reset();
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == OVERFLOW) continue;

                Path name = (Path) event.context();
                Path child = dir.resolve(name);
                if (excludedFileNames.contains(child.getFileName().toString())) continue;

                try {
                    if (kind == ENTRY_CREATE) {
                        if (Files.isDirectory(child)) {
                            registerAll(child); // newly created subdirectory -- watch it too
                        } else {
                            scheduleCheck(child);
                        }
                    } else if (kind == ENTRY_MODIFY) {
                        if (!Files.isDirectory(child)) {
                            scheduleCheck(child);
                        }
                    } else if (kind == ENTRY_DELETE) {
                        handleDelete(child); // nothing to debounce -- it's just gone
                    }
                } catch (IOException e) {
                    System.err.println("FIM error handling " + child + ": " + e.getMessage());
                }
            }

            boolean valid = key.reset();
            if (!valid) {
                watchKeys.remove(key);
            }
        }
    }

    private void scheduleCheck(Path path) {
        Path key = normalize(path);
        if (activeTransferPaths.contains(key)) {
            return; // known transfer activity -- not evaluated, not logged as an event
        }
        ScheduledFuture<?> existing = pendingChecks.get(key);
        if (existing != null) {
            existing.cancel(false); // another event for the same path arrived -- restart the debounce window
        }
        ScheduledFuture<?> future = debounceExecutor.schedule(() -> {
            pendingChecks.remove(key);
            try {
                evaluate(key);
            } catch (IOException e) {
                System.err.println("FIM error evaluating " + key + ": " + e.getMessage());
            }
        }, DEBOUNCE_MILLIS, TimeUnit.MILLISECONDS);
        pendingChecks.put(key, future);
    }

    private void evaluate(Path path) throws IOException {
        if (activeTransferPaths.contains(path)) {
            return; // may have become active during the debounce window
        }
        if (!Files.exists(path)) {
            handleDelete(path);
            return;
        }

        String currentHash;
        try {
            currentHash = ChecksumUtils.sha256Hex(path);
        } catch (IOException e) {
            return; // file mid-write or otherwise unreadable right now -- next event will retry
        }

        Optional<BaselineManager.Entry> known = baseline.get(path);
        if (known.isEmpty()) {
            recordEvent(IntegrityEventType.FILE_CREATED, path, null, currentHash, "new file detected");
            baseline.update(path, currentHash, Files.size(path));
        } else if (!known.get().sha256.equals(currentHash)) {
            recordEvent(IntegrityEventType.FILE_MODIFIED, path, known.get().sha256, currentHash,
                    "content changed vs baseline -- integrity violation");
            // deliberately NOT updating the baseline here -- see class javadoc
        }
        // else: hash matches baseline -- an editor's duplicate-save event with nothing to report
    }

    private void handleDelete(Path path) throws IOException {
        Optional<BaselineManager.Entry> known = baseline.get(path);
        if (known.isPresent()) {
            recordEvent(IntegrityEventType.FILE_DELETED, path, known.get().sha256, null, "file removed");
            baseline.remove(path);
        }
        // deleting something never in the baseline isn't interesting to report
    }

    private void recordEvent(IntegrityEventType type, Path path, String oldHash, String newHash, String message) throws IOException {
        IntegrityEvent event = new IntegrityEvent(System.currentTimeMillis(), path, type, oldHash, newHash, message);
        eventStore.append(event);
        switch (type) {
            case FILE_CREATED -> System.out.println("File created: " + path);
            case FILE_DELETED -> System.out.println("File deleted: " + path);
            case FILE_MODIFIED -> System.out.println("Integrity violation detected: " + path + " (" + message + ")");
        }
    }

    private static Path normalize(Path path) {
        return path.toAbsolutePath().normalize();
    }
}
