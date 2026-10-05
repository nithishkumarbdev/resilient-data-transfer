package com.nithish.filetransfer.integrity;

import com.nithish.filetransfer.ChecksumUtils;
import com.nithish.filetransfer.TransferClient;
import com.nithish.filetransfer.TransferServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class FileIntegrityMonitorTest {

    private static final int PORT = 9878;
    private static final Set<String> EXCLUDED = Set.of("transfer.wal", "integrity-baseline.txt", "integrity-events.log");

    private FileIntegrityMonitor fim;
    private TransferServer server;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.stop();
        if (fim != null) fim.stop();
    }

    @Test
    @DisplayName("Creating a baseline records correct SHA-256 values for existing files")
    void baselineCreation_recordsCorrectHashes() throws Exception {
        Path root = Files.createTempDirectory("fim-baseline");
        Path fileA = root.resolve("a.txt");
        Path fileB = root.resolve("b.txt");
        Files.writeString(fileA, "content of file A");
        Files.writeString(fileB, "content of file B, different");

        BaselineManager baseline = BaselineManager.createBaseline(root, root.resolve("integrity-baseline.txt"));

        assertEquals(ChecksumUtils.sha256Hex(fileA), baseline.get(fileA).orElseThrow().sha256);
        assertEquals(ChecksumUtils.sha256Hex(fileB), baseline.get(fileB).orElseThrow().sha256);
    }

    @Test
    @DisplayName("An unchanged file generates no integrity event")
    void unchangedFile_generatesNoEvent() throws Exception {
        Path root = Files.createTempDirectory("fim-unchanged");
        Files.writeString(root.resolve("stable.txt"), "never touched");

        fim = FileIntegrityMonitor.withFreshBaseline(root,
                root.resolve("integrity-baseline.txt"), root.resolve("integrity-events.log"), EXCLUDED);
        fim.start();

        Thread.sleep(500); // give the watcher a moment; nothing should happen

        List<IntegrityEvent> events = fim.getEventStore().loadAll();
        assertTrue(events.isEmpty(), "Expected no events, got: " + events);
    }

    @Test
    @DisplayName("Modifying a monitored file's content is detected as a violation")
    void modifiedFile_isDetected() throws Exception {
        Path root = Files.createTempDirectory("fim-modify");
        Path file = root.resolve("target.txt");
        Files.writeString(file, "original content");

        fim = FileIntegrityMonitor.withFreshBaseline(root,
                root.resolve("integrity-baseline.txt"), root.resolve("integrity-events.log"), EXCLUDED);
        fim.start();

        Files.writeString(file, "TAMPERED content -- this should not match the baseline");

        waitUntil(() -> !fim.getEventStore().loadAllQuiet().isEmpty());

        List<IntegrityEvent> events = fim.getEventStore().loadAll();
        assertEquals(1, events.size());
        assertEquals(IntegrityEventType.FILE_MODIFIED, events.get(0).type);
        assertEquals(file.toAbsolutePath().normalize(), events.get(0).path);
    }

    @Test
    @DisplayName("Deleting a monitored file generates a FILE_DELETED event")
    void deletedFile_isDetected() throws Exception {
        Path root = Files.createTempDirectory("fim-delete");
        Path file = root.resolve("doomed.txt");
        Files.writeString(file, "will be deleted");

        fim = FileIntegrityMonitor.withFreshBaseline(root,
                root.resolve("integrity-baseline.txt"), root.resolve("integrity-events.log"), EXCLUDED);
        fim.start();

        Files.delete(file);

        waitUntil(() -> !fim.getEventStore().loadAllQuiet().isEmpty());

        List<IntegrityEvent> events = fim.getEventStore().loadAll();
        assertEquals(1, events.size());
        assertEquals(IntegrityEventType.FILE_DELETED, events.get(0).type);
        assertFalse(fim.getBaseline().contains(file));
    }

    @Test
    @DisplayName("A brand new file under the monitored root generates a FILE_CREATED event")
    void newFile_isDetected() throws Exception {
        Path root = Files.createTempDirectory("fim-create");
        fim = FileIntegrityMonitor.withFreshBaseline(root,
                root.resolve("integrity-baseline.txt"), root.resolve("integrity-events.log"), EXCLUDED);
        fim.start();

        Path newFile = root.resolve("brand-new.txt");
        Files.writeString(newFile, "wasn't here before");

        waitUntil(() -> !fim.getEventStore().loadAllQuiet().isEmpty());

        List<IntegrityEvent> events = fim.getEventStore().loadAll();
        assertEquals(1, events.size());
        assertEquals(IntegrityEventType.FILE_CREATED, events.get(0).type);
        assertTrue(fim.getBaseline().contains(newFile), "New file should be added to the baseline once observed");
    }

    @Test
    @DisplayName("Changing content while keeping the same filename is still caught by the hash comparison")
    void contentChangeSameFilename_isCaughtByHash() throws Exception {
        Path root = Files.createTempDirectory("fim-hash");
        Path file = root.resolve("same-name.dat");
        Files.write(file, "version one".getBytes(StandardCharsets.UTF_8));

        BaselineManager baseline = BaselineManager.createBaseline(root, root.resolve("integrity-baseline.txt"));
        String originalHash = baseline.get(file).orElseThrow().sha256;

        Files.write(file, "version two -- completely different bytes, same path".getBytes(StandardCharsets.UTF_8));
        String newHash = ChecksumUtils.sha256Hex(file);

        assertNotEquals(originalHash, newHash, "Precondition: the content actually changed");
        assertEquals(originalHash, baseline.get(file).orElseThrow().sha256,
                "Baseline should still hold the OLD hash until something explicitly updates it");
    }

    @Test
    @DisplayName("A legitimate transfer through TransferServer does not generate a false integrity violation")
    void legitimateTransfer_doesNotTriggerFalseViolation() throws Exception {
        Path outputDir = Files.createTempDirectory("fim-transfer-out");
        Path inputDir = Files.createTempDirectory("fim-transfer-in");

        fim = FileIntegrityMonitor.withExistingBaseline(outputDir,
                outputDir.resolve("integrity-baseline.txt"), outputDir.resolve("integrity-events.log"), EXCLUDED);
        fim.start();

        server = new TransferServer(PORT, outputDir, fim);
        server.start();

        Path input = inputDir.resolve("legit.bin");
        byte[] data = new byte[500_000];
        new SecureRandom().nextBytes(data);
        Files.write(input, data);

        TransferClient.sendFileWithResume("localhost", PORT, input, 3, 0);

        // Give FIM's debounce window plenty of time to settle after the transfer.
        Thread.sleep(700);

        List<IntegrityEvent> events = fim.getEventStore().loadAll();
        boolean anyViolationOnThisFile = events.stream()
                .anyMatch(e -> e.type == IntegrityEventType.FILE_MODIFIED
                        && e.path.equals(outputDir.resolve("legit.bin").toAbsolutePath().normalize()));
        assertFalse(anyViolationOnThisFile, "Legitimate transfer writes should never appear as FILE_MODIFIED violations. Events: " + events);

        BaselineManager.Entry entry = fim.getBaseline().get(outputDir.resolve("legit.bin")).orElseThrow();
        assertEquals(ChecksumUtils.sha256Hex(input), entry.sha256,
                "Baseline should be updated to the transfer's verified checksum");
    }

    @Test
    @DisplayName("The baseline persists across a restart of the monitoring component")
    void baseline_persistsAcrossRestart() throws Exception {
        Path root = Files.createTempDirectory("fim-persist-baseline");
        Path baselineFile = root.resolve("integrity-baseline.txt");
        Path file = root.resolve("persisted.txt");
        Files.writeString(file, "should survive a restart");

        BaselineManager.createBaseline(root, baselineFile);

        // Simulate a restart: a completely new BaselineManager instance loading the same file.
        BaselineManager reloaded = BaselineManager.load(baselineFile);
        assertTrue(reloaded.contains(file));
        assertEquals(ChecksumUtils.sha256Hex(file), reloaded.get(file).orElseThrow().sha256);
    }

    @Test
    @DisplayName("Integrity events persist across a restart of the event store")
    void events_persistAcrossRestart() throws Exception {
        Path root = Files.createTempDirectory("fim-persist-events");
        Path eventLog = root.resolve("integrity-events.log");
        IntegrityEventStore store = new IntegrityEventStore(eventLog);
        store.append(new IntegrityEvent(System.currentTimeMillis(), root.resolve("x.txt"),
                IntegrityEventType.FILE_CREATED, null, "abc123", "test event"));

        // Simulate a restart: a new IntegrityEventStore instance over the same file.
        IntegrityEventStore reloaded = new IntegrityEventStore(eventLog);
        List<IntegrityEvent> events = reloaded.loadAll();
        assertEquals(1, events.size());
        assertEquals(IntegrityEventType.FILE_CREATED, events.get(0).type);
        assertEquals("abc123", events.get(0).newHash);
    }

    @Test
    @DisplayName("Monitoring multiple files and modifying only one only generates an event for that file")
    void multipleFiles_onlyModifiedOneReported() throws Exception {
        Path root = Files.createTempDirectory("fim-multi");
        Path fileA = root.resolve("a.txt");
        Path fileB = root.resolve("b.txt");
        Path fileC = root.resolve("c.txt");
        Files.writeString(fileA, "alpha");
        Files.writeString(fileB, "bravo");
        Files.writeString(fileC, "charlie");

        fim = FileIntegrityMonitor.withFreshBaseline(root,
                root.resolve("integrity-baseline.txt"), root.resolve("integrity-events.log"), EXCLUDED);
        fim.start();

        Files.writeString(fileB, "bravo has been changed");

        waitUntil(() -> !fim.getEventStore().loadAllQuiet().isEmpty());
        Thread.sleep(400); // let any stray debounced checks for a/c settle too

        List<IntegrityEvent> events = fim.getEventStore().loadAll();
        assertEquals(1, events.size(), "Expected exactly one event, got: " + events);
        assertEquals(fileB.toAbsolutePath().normalize(), events.get(0).path);
    }

    // --- helpers ---

    private void waitUntil(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 150 && !condition.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "Condition was never satisfied within timeout");
    }
}
