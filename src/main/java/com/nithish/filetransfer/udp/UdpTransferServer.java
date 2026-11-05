package com.nithish.filetransfer.udp;

import com.google.protobuf.InvalidProtocolBufferException;
import com.nithish.filetransfer.TransferProcessor;
import com.nithish.filetransfer.TransferState;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.TransferMessage;
import com.nithish.filetransfer.proto.UdpAck;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketAddress;
import java.net.SocketException;

/**
 * The UDP transport. Deliberately thin: every "is this chunk safe"
 * decision is delegated to the SAME TransferProcessor the TCP path
 * uses, so a transfer started over UDP and one started over TCP are
 * both just entries in one shared map, backed by one WAL.
 *
 * Unlike TCP, there's no length-prefix framing here -- a UDP datagram
 * already IS one complete message (see the javadoc on TransferMessage
 * in transfer.proto). What UDP does need, and TCP doesn't, is explicit
 * ACKs: TCP's transport already guarantees "the bytes I wrote arrived,
 * in order", so a TCP client only needs to know when it can stop
 * retrying at the WAL/resume layer. A UDP client has no such guarantee
 * from the transport at all -- reliability for each individual
 * datagram has to be built here.
 */
public final class UdpTransferServer {

    /** Generous enough for a chunk payload plus protobuf/field overhead; see UdpTransferClient for the chunk size itself. */
    public static final int RECEIVE_BUFFER_SIZE = 4096;

    private final int port;
    private final TransferProcessor processor;
    private DatagramSocket socket;
    private Thread receiveThread;
    private volatile boolean running;

    public UdpTransferServer(int port, TransferProcessor processor) {
        this.port = port;
        this.processor = processor;
    }

    public void start() throws IOException {
        socket = new DatagramSocket(port);
        running = true;
        receiveThread = new Thread(this::receiveLoop, "udp-transfer-server");
        receiveThread.setDaemon(true);
        receiveThread.start();
        System.out.println("UDP server started on port " + getPort());
    }

    public void stop() {
        running = false;
        if (socket != null) {
            socket.close(); // unblocks receive() with SocketException
        }
        if (receiveThread != null) {
            try { receiveThread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        System.out.println("UDP server stopped");
    }

    /** Actual bound port -- if constructed with 0 (ephemeral), this is what the OS assigned. */
    public int getPort() {
        return socket.getLocalPort();
    }

    private void receiveLoop() {
        byte[] buffer = new byte[RECEIVE_BUFFER_SIZE];
        while (running) {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            try {
                socket.receive(packet);
            } catch (SocketException e) {
                break; // stop() closed the socket
            } catch (IOException e) {
                System.err.println("UDP receive error: " + e.getMessage());
                continue;
            }
            try {
                handlePacket(packet);
            } catch (Exception e) {
                // A malformed or malicious packet must never take the server down --
                // log it and keep serving everyone else.
                System.err.println("UDP error handling packet from " + packet.getSocketAddress() + ": " + e.getMessage());
            }
        }
    }

    private void handlePacket(DatagramPacket packet) throws IOException {
        TransferMessage message;
        try {
            message = TransferMessage.parseFrom(
                    com.google.protobuf.ByteString.copyFrom(packet.getData(), packet.getOffset(), packet.getLength()));
        } catch (InvalidProtocolBufferException e) {
            System.err.println("UDP malformed packet from " + packet.getSocketAddress() + " -- ignored");
            return;
        }

        if (message.hasMetadata()) {
            TransferState state = processor.registerOrGetTransfer(message.getMetadata(), "UDP");
            sendAck(packet.getSocketAddress(), state.transferId, -1);

        } else if (message.hasChunk()) {
            FileChunk chunk = message.getChunk();
            TransferState state = processor.getTransferState(chunk.getTransferId());
            if (state == null) {
                System.err.printf("UDP chunk for unknown transfer_id=%s -- ignored%n", chunk.getTransferId());
                return; // no ack -- client's retry/timeout policy will eventually give up or the metadata will arrive
            }

            TransferProcessor.ChunkOutcome outcome = processor.processChunk(state, chunk);
            if (outcome == TransferProcessor.ChunkOutcome.CHECKSUM_MISMATCH
                    || outcome == TransferProcessor.ChunkOutcome.TRANSFER_ID_MISMATCH) {
                System.err.printf("UDP checksum mismatch: transfer_id=%s seq=%d -- not acked, client will retry%n",
                        chunk.getTransferId(), chunk.getSequenceNumber());
                return; // deliberately no ack -- see class javadoc: no ack means "try again"
            }

            // COMMITTED or DUPLICATE: both get acked. A duplicate is a chunk we
            // already safely committed -- re-acking it (rather than treating it
            // as an error) is exactly what lets a lost ACK self-heal on retry.
            sendAck(packet.getSocketAddress(), chunk.getTransferId(), chunk.getSequenceNumber());
            processor.finalizeIfComplete(state);

        } else {
            System.err.println("UDP unexpected message type from " + packet.getSocketAddress() + " -- ignored");
        }
    }

    private void sendAck(SocketAddress address, String transferId, int sequenceNumber) throws IOException {
        UdpAck ack = UdpAck.newBuilder().setTransferId(transferId).setSequenceNumber(sequenceNumber).build();
        byte[] bytes = TransferMessage.newBuilder().setAck(ack).build().toByteArray();
        socket.send(new DatagramPacket(bytes, bytes.length, address));
    }
}
