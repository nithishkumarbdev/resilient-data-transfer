package com.nithish.filetransfer.metrics;

import com.nithish.filetransfer.ChecksumUtils;
import com.nithish.filetransfer.FileChunker;
import com.nithish.filetransfer.TransferClient;
import com.nithish.filetransfer.TransferServer;
import com.nithish.filetransfer.integrity.FileIntegrityMonitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every assertion here checks that a metric moved because a REAL event
 * happened -- a real transfer, a real restart, a real FIM violation --
 * not that calling some method produces a number. If one of these
 * failed, it would mean the wiring in TransferProcessor/TransferServer/
 * FileIntegrityMonitor doesn't actually call MetricsPublisher where it
 * claims to.
 */
class MetricsIntegrationTest {

    private TransferServer server;
    private FileIntegrityMonitor fim;

    @AfterEach
    void tearDown() throws Exception {
        if (server != null) server.stop();
        if (fim != null) fim.stop();
    }

    @Test
    @DisplayName("A real transfer increments TransfersStarted, TransfersCompleted, and BytesTransferred by real amounts")
    void realTransferDrivesRealMetrics() throws Exception {
        Path outputDir = Files.createTempDirectory("metrics-transfer-out");
        RecordingMetricsPublisher metrics = new RecordingMetricsPublisher();
        server = new TransferServer(0, outputDir, null, metrics);
        server.start();

        Path input = Files.createTempFile("metrics-transfer-in", ".bin");
        byte[] data = new byte[120_000];
        new SecureRandom().nextBytes(data);
        Files.write(input, data);

        TransferClient.TransferOutcome outcome = TransferClient.sendFileWithResume("localhost", server.getPort(), input, 3, 0);
        assertTrue(outcome.success);

        waitUntil(() -> metrics.counterValue("TransfersCompleted") == 1);

        assertEquals(1, metrics.counterValue("TransfersStarted"));
        assertEquals(1, metrics.counterValue("TransfersCompleted"));
        assertEquals(data.length, metrics.counterValue("BytesTransferred"),
                "BytesTransferred should equal the real number of bytes actually written, not an estimate");
        assertTrue(metrics.recordedValueNames.contains("TransferDurationMs"),
                "A real transfer duration should have been recorded");
    }

    @Test
    @DisplayName("A checksum-mismatched chunk increments TransfersFailed, not TransfersCompleted")
    void failedTransferIncrementsFailedCounter() throws Exception {
        Path outputDir = Files.createTempDirectory("metrics-fail-out");
        RecordingMetricsPublisher metrics = new RecordingMetricsPublisher();
        server = new TransferServer(0, outputDir, null, metrics);
        server.start();

        // Hand-craft a bad chunk directly, same technique as TransferIntegrationTest.
        String transferId = "metrics-fail-test";
        byte[] content = "some content that will be corrupted in flight".getBytes();
        try (var socket = new java.net.Socket("localhost", server.getPort());
             var out = new java.io.DataOutputStream(socket.getOutputStream())) {

            var metadata = com.nithish.filetransfer.proto.FileMetadata.newBuilder()
                    .setTransferId(transferId).setFilename("fail.txt")
                    .setTotalSize(content.length).setTotalChunks(1)
                    .setFileChecksum(ChecksumUtils.sha256Hex(content))
                    .setChunkSize(FileChunker.DEFAULT_CHUNK_SIZE).build();
            com.nithish.filetransfer.FramingUtils.writeFrame(out,
                    com.nithish.filetransfer.proto.TransferMessage.newBuilder().setMetadata(metadata).build().toByteArray());

            var chunk = com.nithish.filetransfer.proto.FileChunk.newBuilder()
                    .setTransferId(transferId).setSequenceNumber(0)
                    .setData(com.google.protobuf.ByteString.copyFrom(content))
                    .setChunkChecksum("0".repeat(64)).build();
            com.nithish.filetransfer.FramingUtils.writeFrame(out,
                    com.nithish.filetransfer.proto.TransferMessage.newBuilder().setChunk(chunk).build().toByteArray());
        }

        waitUntil(() -> metrics.counterValue("TransfersFailed") >= 1);
        assertEquals(0, metrics.counterValue("TransfersCompleted"));
    }

    @Test
    @DisplayName("RecoveredTransfers and WALRecoveryDurationMs reflect a real restart, not a fabricated recovery")
    void recoveryDrivesRealMetrics() throws Exception {
        Path outputDir = Files.createTempDirectory("metrics-recovery-out");
        RecordingMetricsPublisher firstRunMetrics = new RecordingMetricsPublisher();
        server = new TransferServer(0, outputDir, null, firstRunMetrics);
        server.start();

        Path input = Files.createTempFile("metrics-recovery-in", ".bin");
        Files.write(input, new byte[FileChunker.DEFAULT_CHUNK_SIZE * 2]); // multi-chunk
        TransferClient.sendFile("localhost", server.getPort(), input); // no resume -- one-shot, completes fully

        waitUntil(() -> firstRunMetrics.counterValue("TransfersCompleted") == 1);

        server.stop();

        // A real restart against the same WAL -- this is what RecoveryManager.recover() actually replays.
        RecordingMetricsPublisher secondRunMetrics = new RecordingMetricsPublisher();
        server = new TransferServer(0, outputDir, null, secondRunMetrics);
        server.start();

        assertEquals(1, secondRunMetrics.counterValue("RecoveredTransfers"));
        assertTrue(secondRunMetrics.recordedValueNames.contains("WALRecoveryDurationMs"));
    }

    @Test
    @DisplayName("A real FIM integrity violation increments the real IntegrityEvents metric")
    void fimViolationDrivesRealMetric() throws Exception {
        Path root = Files.createTempDirectory("metrics-fim-out");
        Path file = root.resolve("watched.txt");
        Files.writeString(file, "original");

        RecordingMetricsPublisher metrics = new RecordingMetricsPublisher();
        fim = FileIntegrityMonitor.withFreshBaseline(root, root.resolve("integrity-baseline.txt"),
                root.resolve("integrity-events.log"), Set.of("transfer.wal", "integrity-baseline.txt", "integrity-events.log"));
        fim.setMetricsPublisher(metrics);
        fim.start();

        Files.writeString(file, "TAMPERED");

        waitUntil(() -> metrics.counterValue("IntegrityEvents") >= 1);
        assertEquals(1, metrics.counterValue("IntegrityEvents"));
    }

    private void waitUntil(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 150 && !condition.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "Condition was never satisfied within timeout");
    }
}
