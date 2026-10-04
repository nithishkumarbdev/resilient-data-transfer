package com.nithish.filetransfer;

import com.google.protobuf.ByteString;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.TransferMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Covers Phase 2 behavior that Phase 1's TransferIntegrationTest doesn't
 * touch: resuming after a restart, duplicate-chunk idempotency, and a
 * clean startup with no prior WAL.
 *
 * NOTE: a real kill -9 crash can't be expressed inside a JUnit test --
 * you can't SIGKILL the JVM the test runner itself is running in without
 * also killing the test. interruptedTransfer_resumesAfterRestart proves
 * the recovery *logic* is correct via a stop()/start() cycle mid-transfer,
 * which exercises the exact same RecoveryManager.recover() path a real
 * restart would. The actual abrupt-crash proof was run separately as a
 * real OS-level kill -9 against a standalone server process -- see the
 * README for that run's captured log output.
 */
class ResumeAndRecoveryTest {

    private static final int PORT = 9877;
    private Path inputDir;
    private Path outputDir;
    private TransferServer server;

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) {
            server.stop();
        }
    }

    @Test
    @DisplayName("A fresh server with no prior WAL starts cleanly with no recovered transfers")
    void freshServer_startsWithNoRecoveredState() throws IOException {
        outputDir = Files.createTempDirectory("resume-test-out");
        server = new TransferServer(PORT, outputDir);
        server.start(); // must not throw, even though transfer.wal doesn't exist yet
        assertNull(server.getTransferState("anything"));
    }

    @Test
    @DisplayName("Sending the same chunk multiple times does not corrupt or duplicate the file")
    void duplicateChunk_isIgnoredIdempotently() throws Exception {
        inputDir = Files.createTempDirectory("resume-test-in");
        outputDir = Files.createTempDirectory("resume-test-out");
        server = new TransferServer(PORT, outputDir);
        server.start();

        Path input = inputDir.resolve("dup.txt");
        Files.writeString(input, "duplicate-chunk idempotency check");
        String transferId = "dup-junit-test";
        byte[] content = Files.readAllBytes(input);
        List<byte[]> chunks = FileChunker.chunkFile(input.toString(), FileChunker.DEFAULT_CHUNK_SIZE);

        try (Socket socket = new Socket("localhost", PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            FileMetadata metadata = FileMetadata.newBuilder()
                    .setTransferId(transferId).setFilename("dup.txt")
                    .setTotalSize(content.length).setTotalChunks(chunks.size())
                    .setFileChecksum(ChecksumUtils.sha256Hex(input)).build();
            FramingUtils.writeFrame(out, TransferMessage.newBuilder().setMetadata(metadata).build().toByteArray());

            // Send chunk 0 three times, simulating a retransmission.
            for (int i = 0; i < 3; i++) {
                FileChunk chunk = FileChunk.newBuilder()
                        .setTransferId(transferId).setSequenceNumber(0)
                        .setData(ByteString.copyFrom(chunks.get(0)))
                        .setChunkChecksum(ChecksumUtils.sha256Hex(chunks.get(0))).build();
                FramingUtils.writeFrame(out, TransferMessage.newBuilder().setChunk(chunk).build().toByteArray());
            }
        }

        waitUntil(() -> {
            TransferState state = server.getTransferState(transferId);
            return state != null && state.isComplete();
        });

        assertArrayEquals(content, Files.readAllBytes(outputDir.resolve("dup.txt")));
    }

    @Test
    @DisplayName("A transfer interrupted by a server restart resumes correctly from the recovered state")
    void interruptedTransfer_resumesAfterRestart() throws Exception {
        inputDir = Files.createTempDirectory("resume-test-in");
        outputDir = Files.createTempDirectory("resume-test-out");
        server = new TransferServer(PORT, outputDir);
        server.start();

        Path input = inputDir.resolve("resume.dat");
        byte[] data = new byte[FileChunker.DEFAULT_CHUNK_SIZE * 4 + 500]; // 5 chunks
        new SecureRandom().nextBytes(data);
        Files.write(input, data);

        // Run the resumable send on a background thread with a small
        // per-chunk delay, so there's a real window to restart the server
        // mid-transfer instead of racing a timing-dependent failure.
        AtomicReference<TransferClient.TransferOutcome> outcomeRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();
        Thread clientThread = new Thread(() -> {
            try {
                outcomeRef.set(TransferClient.sendFileWithResume("localhost", PORT, input, 10, 150));
            } catch (Exception e) {
                errorRef.set(e);
            }
        });
        clientThread.start();

        // Let a couple of chunks land, then restart the server. stop()
        // closes the WAL and socket; the new instance's start() replays
        // the WAL from scratch -- the exact same path a real crash
        // restart takes.
        Thread.sleep(250);
        server.stop();
        TransferServer restarted = new TransferServer(PORT, outputDir);
        restarted.start();
        server = restarted;

        clientThread.join(15_000);

        assertNull(errorRef.get(), "Client thread threw: " + errorRef.get());
        assertNotNull(outcomeRef.get());
        assertTrue(outcomeRef.get().success);
        assertArrayEquals(data, Files.readAllBytes(outputDir.resolve("resume.dat")));
    }

    // --- helpers ---

    private void waitUntil(java.util.function.BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 100 && !condition.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "Condition was never satisfied within timeout");
    }
}
