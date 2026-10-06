package com.nithish.filetransfer;

import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.TransferMessage;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;

public class TransferServer {

    private final int port;
    private final Path outputDir;
    private ServerSocket serverSocket;
    private Thread acceptThread;
    private volatile TransferResult lastResult;

    public TransferServer(int port, Path outputDir) {
        this.port = port;
        this.outputDir = outputDir;
    }

    /** Outcome of the most recently completed transfer — mainly for tests to inspect. */
    public static class TransferResult {
        public final boolean success;
        public final String message;
        public final Path outputPath;

        TransferResult(boolean success, String message, Path outputPath) {
            this.success = success;
            this.message = message;
            this.outputPath = outputPath;
        }
    }

    public void start() throws IOException {
        Files.createDirectories(outputDir);
        serverSocket = new ServerSocket(port);
        acceptThread = new Thread(this::acceptLoop, "transfer-server-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() throws IOException {
        if (serverSocket != null) {
            serverSocket.close();
        }
    }

    public TransferResult getLastResult() {
        return lastResult;
    }

    private void acceptLoop() {
        while (serverSocket != null && !serverSocket.isClosed()) {
            try (Socket socket = serverSocket.accept()) {
                System.out.println("Connection from " + socket.getRemoteSocketAddress());
                lastResult = handleConnection(socket);
                logResult(lastResult);
            } catch (IOException e) {
                if (serverSocket.isClosed()) {
                    break; // stop() was called — this is expected, not an error
                }
                System.err.println("Error handling connection: " + e.getMessage());
            }
        }
    }

    private void logResult(TransferResult result) {
        if (result.success) {
            System.out.println("Transfer OK: " + result.message);
        } else {
            System.out.println("Transfer FAILED: " + result.message);
        }
    }

    /**
     * Reads frames until the sender closes the connection, routing each
     * one by which oneof field is set. Returns a TransferResult rather
     * than throwing, so a bad transfer is reported instead of killing
     * the server (one misbehaving sender shouldn't take down the
     * receiver for everyone else).
     */
    private TransferResult handleConnection(Socket socket) throws IOException {
        DataInputStream in = new DataInputStream(socket.getInputStream());

        FileMetadata metadata = null;
        RandomAccessFile outFile = null;
        int chunksReceived = 0;
        Path outputPath = null;

        try {
            byte[] frame;
            while ((frame = FramingUtils.readFrame(in)) != null) {
                TransferMessage message = TransferMessage.parseFrom(frame);

                if (message.hasMetadata()) {
                    metadata = message.getMetadata();
                    outputPath = outputDir.resolve(metadata.getFilename());
                    outFile = new RandomAccessFile(outputPath.toFile(), "rw");
                    outFile.setLength(metadata.getTotalSize()); // pre-allocate to final size
                    System.out.printf("Receiving %s (%d bytes, %d chunks, transfer_id=%s)%n",
                            metadata.getFilename(), metadata.getTotalSize(),
                            metadata.getTotalChunks(), metadata.getTransferId());

                } else if (message.hasChunk()) {
                    if (metadata == null || outFile == null) {
                        return new TransferResult(false, "Chunk received before metadata", outputPath);
                    }
                    FileChunk chunk = message.getChunk();

                    // Did this chunk survive the trip intact?
                    String actualChecksum = ChecksumUtils.sha256Hex(chunk.getData().toByteArray());
                    if (!actualChecksum.equals(chunk.getChunkChecksum())) {
                        return new TransferResult(false, String.format(
                                "Checksum mismatch on chunk %d: expected %s, got %s",
                                chunk.getSequenceNumber(), chunk.getChunkChecksum(), actualChecksum),
                                outputPath);
                    }

                    // Writing by offset (not by append) means chunks could
                    // in principle arrive out of order and still land
                    // correctly — the property Phase 3's resume will rely on.
                    long offset = (long) chunk.getSequenceNumber() * FileChunker.DEFAULT_CHUNK_SIZE;
                    outFile.seek(offset);
                    outFile.write(chunk.getData().toByteArray());
                    chunksReceived++;
                }
            }

            if (metadata == null) {
                return new TransferResult(false, "Connection closed with no metadata received", null);
            }
            if (chunksReceived != metadata.getTotalChunks()) {
                return new TransferResult(false, String.format(
                        "Expected %d chunks but received %d — transfer incomplete",
                        metadata.getTotalChunks(), chunksReceived), outputPath);
            }

            // Chunk checksums only prove each piece arrived intact.
            // This is the check that proves the *reassembled* file
            // matches the original byte-for-byte.
            String actualFileChecksum = ChecksumUtils.sha256Hex(outputPath);
            boolean match = actualFileChecksum.equals(metadata.getFileChecksum());
            return new TransferResult(match,
                    match ? "integrity check passed (" + metadata.getFilename() + ")"
                          : "integrity check FAILED — file checksum mismatch on " + metadata.getFilename(),
                    outputPath);

        } finally {
            if (outFile != null) {
                outFile.close();
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: TransferServer <port> <outputDir>");
            System.exit(1);
        }
        TransferServer server = new TransferServer(Integer.parseInt(args[0]), Path.of(args[1]));
        server.start();
        System.out.println("Receiver listening on port " + args[0] + ", writing to " + args[1]);
        Thread.currentThread().join(); // keep the process alive; Ctrl+C to stop
    }
}
