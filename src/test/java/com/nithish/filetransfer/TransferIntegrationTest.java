package com.nithish.filetransfer;

import com.google.protobuf.ByteString;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.TransferMessage;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class TransferIntegrationTest {

    // Fixed test port. Fine for a single test class run sequentially;
    // if this suite grows, randomize the port per test instead.
    private static final int PORT = 9876;

    private Path inputDir;
    private Path outputDir;
    private TransferServer server;

    @BeforeEach
    void setUp() throws IOException {
        inputDir = Files.createTempDirectory("transfer-test-in");
        outputDir = Files.createTempDirectory("transfer-test-out");
        server = new TransferServer(PORT, outputDir);
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.stop();
    }

    @Test
    @DisplayName("Small text file transfers and reconstructs identically")
    void smallTextFile() throws Exception {
        Path input = writeInputFile("small.txt", "hello, resilient transfer!".getBytes(StandardCharsets.UTF_8));

        TransferClient.sendFile("localhost", PORT, input);
        waitForResult();

        assertTrue(server.getLastResult().success, server.getLastResult().message);
        assertArrayEquals(Files.readAllBytes(input), Files.readAllBytes(outputDir.resolve("small.txt")));
    }

    @Test
    @DisplayName("Binary file with random bytes transfers correctly")
    void binaryFile() throws Exception {
        byte[] data = new byte[50_000];
        new SecureRandom().nextBytes(data);
        Path input = writeInputFile("binary.dat", data);

        TransferClient.sendFile("localhost", PORT, input);
        waitForResult();

        assertTrue(server.getLastResult().success, server.getLastResult().message);
        assertArrayEquals(data, Files.readAllBytes(outputDir.resolve("binary.dat")));
    }

    @Test
    @DisplayName("Large file spanning multiple chunks (including a partial last chunk) transfers correctly")
    void largeMultiChunkFile() throws Exception {
        // 5 full 64 KB chunks plus a partial 1234-byte chunk.
        byte[] data = new byte[FileChunker.DEFAULT_CHUNK_SIZE * 5 + 1234];
        new SecureRandom().nextBytes(data);
        Path input = writeInputFile("large.dat", data);

        TransferClient.sendFile("localhost", PORT, input);
        waitForResult();

        assertTrue(server.getLastResult().success, server.getLastResult().message);
        assertArrayEquals(data, Files.readAllBytes(outputDir.resolve("large.dat")));
    }

    @Test
    @DisplayName("Empty file transfers as zero chunks and still verifies")
    void emptyFile() throws Exception {
        Path input = writeInputFile("empty.txt", new byte[0]);

        TransferClient.sendFile("localhost", PORT, input);
        waitForResult();

        assertTrue(server.getLastResult().success, server.getLastResult().message);
        assertEquals(0, Files.size(outputDir.resolve("empty.txt")));
    }

    @Test
    @DisplayName("A chunk with a tampered checksum is detected and the transfer is rejected")
    void corruptedChunkIsDetected() throws Exception {
        Path input = writeInputFile("tamper.txt", "some data to corrupt in flight".getBytes(StandardCharsets.UTF_8));

        sendWithBadChunkChecksum(input);
        waitForResult();

        assertFalse(server.getLastResult().success);
        assertTrue(server.getLastResult().message.contains("Checksum mismatch"),
                "Expected a checksum-mismatch failure, got: " + server.getLastResult().message);
    }

    @Test
    @DisplayName("Reconstructed file's whole-file SHA-256 matches the original exactly")
    void endToEndFileChecksumMatches() throws Exception {
        Path input = writeInputFile("checksum-check.bin",
                "another payload block ".repeat(2000).getBytes(StandardCharsets.UTF_8));

        TransferClient.sendFile("localhost", PORT, input);
        waitForResult();

        String expected = ChecksumUtils.sha256Hex(input);
        String actual = ChecksumUtils.sha256Hex(outputDir.resolve("checksum-check.bin"));
        assertEquals(expected, actual);
    }

    // --- helpers ---

    private Path writeInputFile(String name, byte[] content) throws IOException {
        Path file = inputDir.resolve(name);
        Files.write(file, content);
        return file;
    }

    private void waitForResult() throws InterruptedException {
        for (int i = 0; i < 100 && server.getLastResult() == null; i++) {
            Thread.sleep(20);
        }
        assertNotNull(server.getLastResult(), "Server never recorded a result — transfer may have hung");
    }

    /**
     * Bypasses TransferClient to hand-build a metadata + chunk message
     * where the chunk's declared checksum is deliberately wrong,
     * simulating corruption in transit.
     */
    private void sendWithBadChunkChecksum(Path file) throws Exception {
        byte[] content = Files.readAllBytes(file);
        String transferId = UUID.randomUUID().toString();

        try (Socket socket = new Socket("localhost", PORT);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            FileMetadata metadata = FileMetadata.newBuilder()
                    .setTransferId(transferId)
                    .setFilename(file.getFileName().toString())
                    .setTotalSize(content.length)
                    .setTotalChunks(1)
                    .setFileChecksum(ChecksumUtils.sha256Hex(content))
                    .build();
            FramingUtils.writeFrame(out,
                    TransferMessage.newBuilder().setMetadata(metadata).build().toByteArray());

            FileChunk chunk = FileChunk.newBuilder()
                    .setTransferId(transferId)
                    .setSequenceNumber(0)
                    .setData(ByteString.copyFrom(content))
                    .setChunkChecksum("0".repeat(64)) // deliberately wrong SHA-256
                    .build();
            FramingUtils.writeFrame(out,
                    TransferMessage.newBuilder().setChunk(chunk).build().toByteArray());
        }
    }
}
