package com.nithish.filetransfer.udp;

import com.google.protobuf.ByteString;
import com.nithish.filetransfer.ChecksumUtils;
import com.nithish.filetransfer.FileChunker;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.TransferMessage;
import com.nithish.filetransfer.proto.UdpAck;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

/**
 * Stop-and-wait reliability on top of plain UDP: send one datagram,
 * wait (with a timeout) for its specific ACK, retry a bounded number
 * of times, then give up. This is deliberately the simplest reliable
 * scheme, not a sliding window -- the point of this transport is to
 * demonstrate that reliability has to be built at this layer at all,
 * not to out-perform TCP.
 *
 * CHUNK SIZE: much smaller than TCP's 64 KB. A single UDP datagram
 * that large would almost certainly fragment at the IP layer, and this
 * design deliberately avoids depending on fragmentation for correctness
 * -- if any one IP fragment is lost, the whole datagram is lost with no
 * partial-delivery signal. 1024 bytes keeps a whole chunk (plus its
 * protobuf/field overhead) safely under a typical 1500-byte Ethernet
 * MTU.
 */
public final class UdpTransferClient {

    public static final int UDP_CHUNK_SIZE = 1024;

    public static final class Outcome {
        public final String transferId;
        public final boolean success;
        public final int totalRetries;

        Outcome(String transferId, boolean success, int totalRetries) {
            this.transferId = transferId;
            this.success = success;
            this.totalRetries = totalRetries;
        }
    }

    /**
     * @param timeoutMillis how long to wait for an ACK before retrying
     * @param maxRetriesPerMessage bounded retries -- per DATA/metadata message, not for the whole transfer
     */
    public static Outcome sendFile(String host, int port, Path filepath,
                                    int timeoutMillis, int maxRetriesPerMessage) throws IOException {
        if (!Files.exists(filepath)) {
            throw new IOException("File not found: " + filepath);
        }

        String transferId = UUID.randomUUID().toString();
        String filename = filepath.getFileName().toString();
        long totalSize = Files.size(filepath);
        String fileChecksum = ChecksumUtils.sha256Hex(filepath);
        List<byte[]> chunks = FileChunker.chunkFile(filepath.toString(), UDP_CHUNK_SIZE);
        int totalChunks = chunks.size();

        InetAddress address = InetAddress.getByName(host);
        int totalRetries = 0;

        try (DatagramSocket socket = new DatagramSocket()) {
            socket.setSoTimeout(timeoutMillis);

            // --- START: metadata, awaiting ack sequence_number == -1 ---
            FileMetadata metadata = FileMetadata.newBuilder()
                    .setTransferId(transferId).setFilename(filename).setTotalSize(totalSize)
                    .setTotalChunks(totalChunks).setFileChecksum(fileChecksum).setChunkSize(UDP_CHUNK_SIZE)
                    .build();
            byte[] metadataBytes = TransferMessage.newBuilder().setMetadata(metadata).build().toByteArray();
            totalRetries += sendUntilAcked(socket, address, port, metadataBytes, transferId, -1, maxRetriesPerMessage);

            // --- DATA: one chunk at a time, awaiting its specific ack ---
            for (int i = 0; i < totalChunks; i++) {
                FileChunk chunk = FileChunk.newBuilder()
                        .setTransferId(transferId).setSequenceNumber(i)
                        .setData(ByteString.copyFrom(chunks.get(i)))
                        .setChunkChecksum(ChecksumUtils.sha256Hex(chunks.get(i)))
                        .build();
                byte[] chunkBytes = TransferMessage.newBuilder().setChunk(chunk).build().toByteArray();
                totalRetries += sendUntilAcked(socket, address, port, chunkBytes, transferId, i, maxRetriesPerMessage);
            }
        }

        return new Outcome(transferId, true, totalRetries);
    }

    /** Sends payload, waiting for a matching ack, retrying on timeout. Returns the number of retries actually used. Throws if exhausted. */
    private static int sendUntilAcked(DatagramSocket socket, InetAddress address, int port, byte[] payload,
                                       String transferId, int expectedSeq, int maxRetries) throws IOException {
        DatagramPacket outPacket = new DatagramPacket(payload, payload.length, address, port);
        byte[] recvBuffer = new byte[UdpTransferServer.RECEIVE_BUFFER_SIZE];

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            socket.send(outPacket);

            long deadline = System.currentTimeMillis() + socket.getSoTimeout();
            while (System.currentTimeMillis() < deadline) {
                DatagramPacket inPacket = new DatagramPacket(recvBuffer, recvBuffer.length);
                try {
                    socket.receive(inPacket);
                } catch (SocketTimeoutException e) {
                    break; // fall through to retry
                }
                TransferMessage response;
                try {
                    response = TransferMessage.parseFrom(
                            com.google.protobuf.ByteString.copyFrom(inPacket.getData(), inPacket.getOffset(), inPacket.getLength()));
                } catch (Exception e) {
                    continue; // garbage packet -- keep waiting within this attempt's window
                }
                if (!response.hasAck()) continue;
                UdpAck ack = response.getAck();
                if (ack.getTransferId().equals(transferId) && ack.getSequenceNumber() == expectedSeq) {
                    return attempt; // acked -- attempt N means N retries were needed (attempt 0 = no retry)
                }
                // An ack for a different (older) sequence number -- a late/duplicate
                // ack from a previous retry. Ignore it and keep waiting for THIS one.
            }
            System.out.printf("UDP timeout waiting for ack(transfer_id=%s, seq=%d), attempt %d/%d%n",
                    transferId, expectedSeq, attempt + 1, maxRetries + 1);
        }
        throw new IOException(String.format(
                "UDP transfer failed: no ack for seq=%d after %d attempt(s)", expectedSeq, maxRetries + 1));
    }
}
