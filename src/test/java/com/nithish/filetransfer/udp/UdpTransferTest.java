package com.nithish.filetransfer.udp;

import com.google.protobuf.ByteString;
import com.nithish.filetransfer.*;
import com.nithish.filetransfer.api.ManagementApiServer;
import com.nithish.filetransfer.integrity.FileIntegrityMonitor;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.TransferMessage;
import com.nithish.filetransfer.proto.UdpAck;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class UdpTransferTest {

    private UdpTransferServer udpServer;
    private TransferProcessor processor;
    private FileIntegrityMonitor fim;
    private TransferServer tcpServer; // only used by the API-integration test, for its TransferProcessor wiring

    @AfterEach
    void tearDown() throws Exception {
        if (udpServer != null) udpServer.stop();
        if (tcpServer != null) tcpServer.stop();
        if (processor != null && tcpServer == null) processor.close(); // tcpServer.stop() already closes its own processor
        if (fim != null) fim.stop();
    }

    private UdpTransferServer newServer(Path outputDir) throws IOException {
        processor = new TransferProcessor(outputDir, null);
        processor.initialize();
        udpServer = new UdpTransferServer(0, processor); // ephemeral port
        udpServer.start();
        return udpServer;
    }

    @Test
    @DisplayName("A small file transfers correctly over UDP and the reconstructed file's SHA-256 matches")
    void normalUdpTransfer_smallFile() throws Exception {
        Path outputDir = Files.createTempDirectory("udp-normal-out");
        Path input = Files.createTempFile("udp-normal-in", ".txt");
        Files.writeString(input, "a small file sent over UDP with stop-and-wait reliability");

        newServer(outputDir);
        UdpTransferClient.Outcome outcome = UdpTransferClient.sendFile("localhost", udpServer.getPort(), input, 500, 5);

        assertTrue(outcome.success);
        Path output = outputDir.resolve(input.getFileName().toString());
        waitUntil(() -> Files.exists(output));
        assertEquals(ChecksumUtils.sha256Hex(input), ChecksumUtils.sha256Hex(output));
    }

    @Test
    @DisplayName("A multi-chunk file (forcing many small UDP datagrams) assembles correctly")
    void multiChunkFile_assemblesCorrectly() throws Exception {
        Path outputDir = Files.createTempDirectory("udp-multi-out");
        Path input = Files.createTempFile("udp-multi-in", ".bin");
        byte[] data = new byte[UdpTransferClient.UDP_CHUNK_SIZE * 6 + 200]; // 7 chunks
        new SecureRandom().nextBytes(data);
        Files.write(input, data);

        newServer(outputDir);
        UdpTransferClient.Outcome outcome = UdpTransferClient.sendFile("localhost", udpServer.getPort(), input, 500, 5);

        assertTrue(outcome.success);
        Path output = outputDir.resolve(input.getFileName().toString());
        waitUntil(() -> {
            TransferState state = processor.getTransferState(outcome.transferId);
            return state != null && state.isComplete();
        });
        assertArrayEquals(data, Files.readAllBytes(output));
    }

    @Test
    @DisplayName("A chunk with a bad checksum gets no ACK and is never committed; the correct resend succeeds")
    void checksumMismatch_notAckedNotCommitted() throws Exception {
        Path outputDir = Files.createTempDirectory("udp-badsum-out");
        newServer(outputDir);

        try (DatagramSocket client = new DatagramSocket()) {
            client.setSoTimeout(300);
            InetAddress addr = InetAddress.getByName("localhost");
            String transferId = UUID.randomUUID().toString();
            byte[] chunkData = "twelve bytes".getBytes(); // 12 bytes

            sendMetadata(client, addr, udpServer.getPort(), transferId, "badsum.txt", chunkData.length, 1, ChecksumUtils.sha256Hex(chunkData));
            UdpAck startAck = receiveAck(client);
            assertEquals(-1, startAck.getSequenceNumber());

            // Send chunk 0 with a deliberately WRONG checksum.
            FileChunk badChunk = FileChunk.newBuilder()
                    .setTransferId(transferId).setSequenceNumber(0)
                    .setData(ByteString.copyFrom(chunkData))
                    .setChunkChecksum("0".repeat(64))
                    .build();
            sendRaw(client, addr, udpServer.getPort(), TransferMessage.newBuilder().setChunk(badChunk).build());

            // No ack should arrive for the bad chunk.
            assertThrows(SocketTimeoutException.class, () -> receiveAck(client));

            TransferState state = processor.getTransferState(transferId);
            assertFalse(state.isCommitted(0), "A checksum-mismatched chunk must never be committed");

            // Now resend with the CORRECT checksum -- should succeed normally.
            FileChunk goodChunk = FileChunk.newBuilder()
                    .setTransferId(transferId).setSequenceNumber(0)
                    .setData(ByteString.copyFrom(chunkData))
                    .setChunkChecksum(ChecksumUtils.sha256Hex(chunkData))
                    .build();
            sendRaw(client, addr, udpServer.getPort(), TransferMessage.newBuilder().setChunk(goodChunk).build());
            UdpAck ack0 = receiveAck(client);
            assertEquals(0, ack0.getSequenceNumber());
            assertTrue(state.isCommitted(0));
        }
    }

    @Test
    @DisplayName("A duplicate/retransmitted packet (simulating a lost ACK) is re-acked, not double-written")
    void duplicatePacket_lostAckSelfHeals() throws Exception {
        Path outputDir = Files.createTempDirectory("udp-dup-out");
        newServer(outputDir);

        try (DatagramSocket client = new DatagramSocket()) {
            client.setSoTimeout(300);
            InetAddress addr = InetAddress.getByName("localhost");
            String transferId = UUID.randomUUID().toString();
            byte[] chunkData = "duplicate this chunk please".getBytes();

            sendMetadata(client, addr, udpServer.getPort(), transferId, "dup.txt", chunkData.length, 1, ChecksumUtils.sha256Hex(chunkData));
            receiveAck(client); // start ack

            FileChunk chunk = FileChunk.newBuilder()
                    .setTransferId(transferId).setSequenceNumber(0)
                    .setData(ByteString.copyFrom(chunkData))
                    .setChunkChecksum(ChecksumUtils.sha256Hex(chunkData))
                    .build();
            TransferMessage msg = TransferMessage.newBuilder().setChunk(chunk).build();

            // First send: server commits it, sends an ack. We deliberately
            // don't rely on that ack having reached us -- simulating it
            // being lost is exactly "resend the same chunk again".
            sendRaw(client, addr, udpServer.getPort(), msg);
            UdpAck firstAck = receiveAck(client);
            assertEquals(0, firstAck.getSequenceNumber());

            // Resend the EXACT same chunk (what the client would do after a lost ack).
            sendRaw(client, addr, udpServer.getPort(), msg);
            UdpAck secondAck = receiveAck(client);
            assertEquals(0, secondAck.getSequenceNumber(), "Server should re-ack a duplicate, not error out");

            Path output = outputDir.resolve("dup.txt");
            assertEquals(chunkData.length, Files.size(output), "File size must not double from the duplicate");
            assertArrayEquals(chunkData, Files.readAllBytes(output));
        }
    }

    @Test
    @DisplayName("With no server responding, the client retries the bounded number of times then fails")
    void timeoutAndRetryExhaustion_failsCleanly() throws Exception {
        // A "silent" receiver: reads datagrams but deliberately never acks,
        // so we can count exactly how many times the client retried.
        try (DatagramSocket silentServer = new DatagramSocket(0)) {
            int[] receivedCount = {0};
            Thread listener = new Thread(() -> {
                byte[] buf = new byte[4096];
                while (!silentServer.isClosed()) {
                    try {
                        DatagramPacket p = new DatagramPacket(buf, buf.length);
                        silentServer.receive(p);
                        synchronized (receivedCount) { receivedCount[0]++; }
                    } catch (IOException e) {
                        break;
                    }
                }
            });
            listener.setDaemon(true);
            listener.start();

            Path input = Files.createTempFile("udp-timeout-in", ".txt");
            Files.writeString(input, "this will never be acked");

            int maxRetries = 2;
            IOException thrown = assertThrows(IOException.class, () ->
                    UdpTransferClient.sendFile("localhost", silentServer.getLocalPort(), input, 150, maxRetries));
            assertTrue(thrown.getMessage().contains("no ack"), "Expected a clear failure reason, got: " + thrown.getMessage());

            silentServer.close();
            listener.join(1000);

            // maxRetries=2 means 3 total attempts (0, 1, 2) for the metadata message before giving up.
            synchronized (receivedCount) {
                assertEquals(maxRetries + 1, receivedCount[0],
                        "Expected exactly " + (maxRetries + 1) + " attempts (initial + retries), got " + receivedCount[0]);
            }
        }
    }

    @Test
    @DisplayName("Chunks sent out of order (0, 2, 1) still assemble into the correct file")
    void outOfOrderChunks_assembleCorrectly() throws Exception {
        Path outputDir = Files.createTempDirectory("udp-ooo-out");
        newServer(outputDir);

        try (DatagramSocket client = new DatagramSocket()) {
            client.setSoTimeout(500);
            InetAddress addr = InetAddress.getByName("localhost");
            String transferId = UUID.randomUUID().toString();

            int chunkSize = 10;
            byte[] c0 = "AAAAAAAAAA".getBytes();
            byte[] c1 = "BBBBBBBBBB".getBytes();
            byte[] c2 = "CCCCCCCCCC".getBytes();
            byte[] whole = new byte[30];
            System.arraycopy(c0, 0, whole, 0, 10);
            System.arraycopy(c1, 0, whole, 10, 10);
            System.arraycopy(c2, 0, whole, 20, 10);
            String fileChecksum = ChecksumUtils.sha256Hex(whole);

            sendMetadataWithChunkSize(client, addr, udpServer.getPort(), transferId, "ooo.bin", 30, 3, fileChecksum, chunkSize);
            receiveAck(client);

            sendChunkAndExpectAck(client, addr, udpServer.getPort(), transferId, 0, c0);
            sendChunkAndExpectAck(client, addr, udpServer.getPort(), transferId, 2, c2);
            sendChunkAndExpectAck(client, addr, udpServer.getPort(), transferId, 1, c1);

            Path output = outputDir.resolve("ooo.bin");
            waitUntil(() -> {
                TransferState state = processor.getTransferState(transferId);
                return state != null && state.isComplete();
            });
            assertArrayEquals(whole, Files.readAllBytes(output));
            assertEquals(fileChecksum, ChecksumUtils.sha256Hex(output));
        }
    }

    @Test
    @DisplayName("A UDP transfer interrupted mid-way is recovered correctly from the WAL after a restart")
    void udpTransfer_recoversFromWalAfterRestart() throws Exception {
        Path outputDir = Files.createTempDirectory("udp-recovery-out");
        newServer(outputDir);

        String transferId;
        int chunkSize = 10;
        byte[] c0 = "0000000000".getBytes();
        byte[] c1 = "1111111111".getBytes();
        byte[] c2 = "2222222222".getBytes();
        byte[] whole = new byte[30];
        System.arraycopy(c0, 0, whole, 0, 10);
        System.arraycopy(c1, 0, whole, 10, 10);
        System.arraycopy(c2, 0, whole, 20, 10);
        String fileChecksum = ChecksumUtils.sha256Hex(whole);

        try (DatagramSocket client = new DatagramSocket()) {
            client.setSoTimeout(500);
            InetAddress addr = InetAddress.getByName("localhost");
            transferId = UUID.randomUUID().toString();

            sendMetadataWithChunkSize(client, addr, udpServer.getPort(), transferId, "recover.bin", 30, 3, fileChecksum, chunkSize);
            receiveAck(client);
            sendChunkAndExpectAck(client, addr, udpServer.getPort(), transferId, 0, c0);
            sendChunkAndExpectAck(client, addr, udpServer.getPort(), transferId, 1, c1);
            // Chunk 2 deliberately NOT sent -- simulates the crash happening here.
        }

        // Simulate a restart: stop this server/processor, build a NEW
        // TransferProcessor + UdpTransferServer against the same outputDir,
        // forcing a real RecoveryManager.recover() replay of the WAL.
        udpServer.stop();
        processor.close();

        TransferProcessor recoveredProcessor = new TransferProcessor(outputDir, null);
        recoveredProcessor.initialize();
        TransferState recovered = recoveredProcessor.getTransferState(transferId);
        assertNotNull(recovered);
        assertEquals("UDP", recovered.transport);
        assertEquals(chunkSize, recovered.chunkSize);
        assertEquals(1, recovered.highestContiguousCommitted(), "Chunks 0 and 1 should have survived; chunk 2 was never sent");

        UdpTransferServer recoveredServer = new UdpTransferServer(0, recoveredProcessor);
        recoveredServer.start();
        try (DatagramSocket client = new DatagramSocket()) {
            client.setSoTimeout(500);
            InetAddress addr = InetAddress.getByName("localhost");
            sendChunkAndExpectAck(client, addr, recoveredServer.getPort(), transferId, 2, c2);
        }

        Path output = outputDir.resolve("recover.bin");
        waitUntil(() -> recoveredProcessor.getTransferState(transferId).isComplete());
        assertArrayEquals(whole, Files.readAllBytes(output));

        recoveredServer.stop();
        recoveredProcessor.close();
    }

    @Test
    @DisplayName("A UDP-originated transfer shows transport=UDP through the same HTTP API used for TCP")
    void apiReflectsUdpTransfer_showsTransportField() throws Exception {
        Path outputDir = Files.createTempDirectory("udp-api-out");
        fim = FileIntegrityMonitor.withExistingBaseline(outputDir,
                outputDir.resolve("integrity-baseline.txt"), outputDir.resolve("integrity-events.log"),
                java.util.Set.of("transfer.wal", "integrity-baseline.txt", "integrity-events.log"));
        fim.start();
        tcpServer = new TransferServer(0, outputDir, fim); // owns the TransferProcessor we'll share with UDP
        tcpServer.start();
        processor = tcpServer.getProcessor();
        udpServer = new UdpTransferServer(0, processor);
        udpServer.start();
        ManagementApiServer api = new ManagementApiServer(tcpServer, fim);
        api.start(0);

        try {
            Path input = Files.createTempFile("udp-api-in", ".txt");
            Files.writeString(input, "a file sent over UDP, checked via the HTTP API");
            UdpTransferClient.Outcome outcome = UdpTransferClient.sendFile("localhost", udpServer.getPort(), input, 500, 5);
            assertTrue(outcome.success);

            waitUntil(() -> {
                TransferState state = processor.getTransferState(outcome.transferId);
                return state != null && state.isComplete();
            });

            HttpClient http = HttpClient.newHttpClient();
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + api.getPort() + "/transfers/" + outcome.transferId)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, response.statusCode());
            assertTrue(response.body().contains("\"transport\":\"UDP\""), "Expected transport:UDP in: " + response.body());
        } finally {
            api.stop();
        }
    }

    // --- raw UDP protocol helpers ---

    private static void sendMetadata(DatagramSocket socket, InetAddress addr, int port, String transferId,
                                      String filename, long totalSize, int totalChunks, String fileChecksum) throws IOException {
        sendMetadataWithChunkSize(socket, addr, port, transferId, filename, totalSize, totalChunks, fileChecksum, UdpTransferClient.UDP_CHUNK_SIZE);
    }

    private static void sendMetadataWithChunkSize(DatagramSocket socket, InetAddress addr, int port, String transferId,
                                                    String filename, long totalSize, int totalChunks, String fileChecksum,
                                                    int chunkSize) throws IOException {
        FileMetadata metadata = FileMetadata.newBuilder()
                .setTransferId(transferId).setFilename(filename).setTotalSize(totalSize)
                .setTotalChunks(totalChunks).setFileChecksum(fileChecksum).setChunkSize(chunkSize)
                .build();
        sendRaw(socket, addr, port, TransferMessage.newBuilder().setMetadata(metadata).build());
    }

    private static void sendChunkAndExpectAck(DatagramSocket socket, InetAddress addr, int port, String transferId,
                                               int seq, byte[] data) throws IOException {
        FileChunk chunk = FileChunk.newBuilder()
                .setTransferId(transferId).setSequenceNumber(seq)
                .setData(ByteString.copyFrom(data))
                .setChunkChecksum(ChecksumUtils.sha256Hex(data))
                .build();
        sendRaw(socket, addr, port, TransferMessage.newBuilder().setChunk(chunk).build());
        UdpAck ack = receiveAck(socket);
        assertEquals(seq, ack.getSequenceNumber());
    }

    private static void sendRaw(DatagramSocket socket, InetAddress addr, int port, TransferMessage message) throws IOException {
        byte[] bytes = message.toByteArray();
        socket.send(new DatagramPacket(bytes, bytes.length, addr, port));
    }

    private static UdpAck receiveAck(DatagramSocket socket) throws IOException {
        byte[] buf = new byte[UdpTransferServer.RECEIVE_BUFFER_SIZE];
        DatagramPacket packet = new DatagramPacket(buf, buf.length);
        socket.receive(packet); // throws SocketTimeoutException if nothing arrives -- used deliberately in some tests
        TransferMessage message = TransferMessage.parseFrom(ByteString.copyFrom(packet.getData(), packet.getOffset(), packet.getLength()));
        assertTrue(message.hasAck(), "Expected an ack message");
        return message.getAck();
    }

    private void waitUntil(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 150 && !condition.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "Condition was never satisfied within timeout");
    }
}
