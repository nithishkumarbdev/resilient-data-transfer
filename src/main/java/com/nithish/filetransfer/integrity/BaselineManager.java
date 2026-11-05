package com.nithish.filetransfer.integrity;

import com.nithish.filetransfer.ChecksumUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * The trusted baseline: for each monitored file, the SHA-256 and size we
 * expect it to have. Persisted as one line per file --
 * "path|sha256|size|lastVerifiedMillis" -- rewritten in full on every
 * update. That's simple rather than efficient, but a baseline of a
 * monitored directory is small enough that this is genuinely fine, and
 * a plain rewrite is much easier to reason about than an in-place
 * text-file edit.
 *
 * INTERVIEW-IMPORTANT: this reuses ChecksumUtils.sha256Hex(Path), the
 * exact same hashing utility Phase 1's transfer verification uses --
 * there's no second SHA-256 implementation anywhere in this project.
 */
public final class BaselineManager {

    public static final class Entry {
        public final String sha256;
        public final long size;
        public final long lastVerifiedMillis;

        public Entry(String sha256, long size, long lastVerifiedMillis) {
            this.sha256 = sha256;
            this.size = size;
            this.lastVerifiedMillis = lastVerifiedMillis;
        }
    }

    private final Path baselineFile;
    private final Map<Path, Entry> entries;

    private BaselineManager(Path baselineFile, Map<Path, Entry> entries) {
        this.baselineFile = baselineFile;
        this.entries = entries;
    }

    /** Loads an existing baseline file, or returns an empty baseline if none exists yet. */
    public static BaselineManager load(Path baselineFile) throws IOException {
        Map<Path, Entry> entries = new LinkedHashMap<>();
        if (Files.exists(baselineFile)) {
            for (String line : Files.readAllLines(baselineFile, StandardCharsets.UTF_8)) {
                if (line.isBlank()) continue;
                String[] parts = line.split("\\|", 4);
                if (parts.length != 4) continue; // skip a malformed line rather than fail startup
                Path path = Path.of(parts[0]);
                entries.put(path, new Entry(parts[1], Long.parseLong(parts[2]), Long.parseLong(parts[3])));
            }
        }
        return new BaselineManager(baselineFile, entries);
    }

    /**
     * Explicit initial-baseline operation: scans every regular file under
     * root, hashes it, and writes a fresh baseline. This is deliberately
     * a separate, named operation from load() -- establishing trust in
     * whatever happens to be on disk right now is a decision the caller
     * makes on purpose, not something that happens implicitly on startup.
     */
    public static BaselineManager createBaseline(Path root, Path baselineFile) throws IOException {
        return createBaseline(root, baselineFile, java.util.Set.of());
    }

    /**
     * excludedFileNames lets the caller keep infrastructure files (the WAL,
     * the event log, the baseline file itself) out of the baseline -- FIM
     * protects transferred files, not the system's own operational state.
     * Matched by filename only, since these always live directly under root.
     */
    public static BaselineManager createBaseline(Path root, Path baselineFile, java.util.Set<String> excludedFileNames) throws IOException {
        Map<Path, Entry> entries = new LinkedHashMap<>();
        if (Files.exists(root)) {
            try (Stream<Path> walk = Files.walk(root)) {
                for (Path file : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
                    if (excludedFileNames.contains(file.getFileName().toString())) continue;
                    Path key = file.toAbsolutePath().normalize();
                    if (key.equals(baselineFile.toAbsolutePath().normalize())) continue; // don't baseline ourselves
                    entries.put(key, new Entry(ChecksumUtils.sha256Hex(file), Files.size(file), System.currentTimeMillis()));
                }
            }
        }
        BaselineManager manager = new BaselineManager(baselineFile, entries);
        manager.persist();
        return manager;
    }

    public synchronized Optional<Entry> get(Path path) {
        return Optional.ofNullable(entries.get(normalize(path)));
    }

    public synchronized boolean contains(Path path) {
        return entries.containsKey(normalize(path));
    }

    public synchronized void update(Path path, String sha256, long size) throws IOException {
        entries.put(normalize(path), new Entry(sha256, size, System.currentTimeMillis()));
        persist();
    }

    public synchronized void remove(Path path) throws IOException {
        entries.remove(normalize(path));
        persist();
    }

    public synchronized Map<Path, Entry> snapshot() {
        return new LinkedHashMap<>(entries);
    }

    private static Path normalize(Path path) {
        return path.toAbsolutePath().normalize();
    }

    private void persist() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Path, Entry> e : entries.entrySet()) {
            Entry v = e.getValue();
            sb.append(e.getKey()).append('|').append(v.sha256).append('|')
              .append(v.size).append('|').append(v.lastVerifiedMillis).append('\n');
        }
        Files.writeString(baselineFile, sb.toString(), StandardCharsets.UTF_8);
    }
}
