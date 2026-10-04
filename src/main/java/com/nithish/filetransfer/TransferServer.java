package com.nithish.filetransfer;

import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.ResumeRequest;
import com.nithish.filetransfer.proto.ResumeResponse;
import com.nithish.filetransfer.proto.TransferMessage;
import com.nithish.filetransfer.proto.WalChunkCommitted;
import com.nithish.filetransfer.proto.WalEntry;
import com.nithish.filetransfer.proto.WalTransferStart;
import com.nithish.filetransfer.wal.WalWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TransferServer {

    private final int port;
    private final Path outputDir;
    private final Path walPath;

    private ServerSocket serverSocket;
    private Thread acceptThread;
    private WalWriter wal;
    private Map<String, TransferState> transfers;
    private volatile TransferResult lastResult;

    public TransferServer(int port, Path outputDir) {
        this.port = port;
        this.outputDir = outputDir;
        this.walPath = outputDir.resolve("transfer.wal");
    }

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

        // Recovery happens before we accept a single connection: rebuild
        // whatever state existed when we last shut down (cleanly or not).
        transfers = new ConcurrentHashMap<>(RecoveryManager.recover(walPath, outputDir));
        if (!transfers.isEmpty()) {
            System.out.println("Recovered " + transfers.size() + " transfer(s) from WAL:");
            for (TransferState s : transfers.values()) {
                System.out.printf("  transfer_id=%s  %d/%d chunks contiguous-committed%n",
                        s.transferId, s.highestContiguousCommitted() + 1, s.totalChunks);
            }
        }

        wal = new WalWriter(walPath, WalWriter.FlushPolicy.EVERY_RECORD);

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true); // allow immediate rebind after stop() -- avoids a TIME_WAIT race on quick restarts
        serverSocket.bind(new java.net.InetSocketAddress(port));
        acceptThread = new Thread(this::acceptLoop, "transfer-server-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public void stop() throws IOException {
        if (serverSocket != null) {
            serverSocket.close();
        }
        if (acceptThread != null) {
            try {
                acceptThread.join(2000); // wait for the accept loop to actually exit before returning
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (wal != null) {
            wal.close();
        }
    }

    public TransferResult getLastResult() {
        return lastResult;
    }

    public TransferState getTransferState(String transferId) {
        return transfers.get(transferId);
    }

    private void acceptLoop() {
        while (serverSocket != null && !serverSocket.isClosed()) {
            try (Socket socket = serverSocket.accept()) {
                System.out.println("Connection from " + socket.getRemoteSocketAddress());
                lastResult = handleConnection(socket);
                logResult(lastResult);
            } catch (IOException e) {
                if (serverSocket.isClosed()) {
                    break;
                }
                System.err.println("Error handling connection: " + e.getMessage());
            }
        }
    }

    private void logResult(TransferResult result) {
        System.out.println((result.success ? "Transfer OK: " : "Transfer incomplete/failed: ") + result.message);
    }

    /**
     * The first message on a connection determines what's happening:
     * FileMetadata starts a (possibly brand new) transfer, ResumeRequest
     * asks where a known transfer left off. Everything after that is
     * FileChunk messages until the connection closes.
     */
    private TransferResult handleConnection(Socket socket) throws IOException {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        byte[] firstFrame = FramingUtils.readFrame(in);
        if (firstFrame == null) {
            return new TransferResult(false, "connection closed with no messages", null);
        }
        TransferMessage firstMessage = TransferMessage.parseFrom(firstFrame);

        TransferState state;
        if (firstMessage.hasMetadata()) {
            state = handleNewTransfer(firstMessage.getMetadata());
        } else if (firstMessage.hasResumeRequest()) {
            state = handleResumeRequest(firstMessage.getResumeRequest(), out);
            if (state == null) {
                return new TransferResult(false, "resume requested for unknown transfer_id", null);
            }
        } else {
            return new TransferResult(false, "first message on a connection must be metadata or resume_request", null);
        }

        return receiveChunks(state, in);
    }

    private TransferState handleNewTransfer(FileMetadata metadata) throws IOException {
        TransferState existing = transfers.get(metadata.getTransferId());
        if (existing != null) {
            // Metadata resent for a transfer we already know about -- idempotent, just reuse it.
            return existing;
        }

        Path outputPath = outputDir.resolve(metadata.getFilename());
        TransferState state = new TransferState(metadata.getTransferId(), metadata.getFilename(),
                metadata.getTotalSize(), metadata.getTotalChunks(), metadata.getFileChecksum(), outputPath);

        try (RandomAccessFile file = new RandomAccessFile(outputPath.toFile(), "rw")) {
            file.setLength(metadata.getTotalSize()); // pre-allocate to final size
        }

        WalTransferStart walStart = WalTransferStart.newBuilder()
                .setTransferId(metadata.getTransferId())
                .setFilename(metadata.getFilename())
                .setTotalSize(metadata.getTotalSize())
                .setTotalChunks(metadata.getTotalChunks())
                .setFileChecksum(metadata.getFileChecksum())
                .build();
        wal.append(WalEntry.newBuilder().setTransferStart(walStart).build().toByteArray());

        transfers.put(metadata.getTransferId(), state);
        System.out.printf("Transfer started: %s (%d bytes, %d chunks, transfer_id=%s)%n",
                metadata.getFilename(), metadata.getTotalSize(), metadata.getTotalChunks(), metadata.getTransferId());
        return state;
    }

    private TransferState handleResumeRequest(ResumeRequest request, DataOutputStream out) throws IOException {
        TransferState state = transfers.get(request.getTransferId());
        int lastCommitted = (state == null) ? -1 : state.highestContiguousCommitted();

        ResumeResponse response = ResumeResponse.newBuilder()
                .setTransferId(request.getTransferId())
                .setLastCommittedSequence(lastCommitted)
                .build();
        FramingUtils.writeFrame(out, TransferMessage.newBuilder().setResumeResponse(response).build().toByteArray());

        System.out.printf("Resume requested: transfer_id=%s -> last_committed_sequence=%d%n",
                request.getTransferId(), lastCommitted);
        return state;
    }

    private TransferResult receiveChunks(TransferState state, DataInputStream in) throws IOException {
        byte[] frame;
        while ((frame = FramingUtils.readFrame(in)) != null) {
            TransferMessage message = TransferMessage.parseFrom(frame);
            if (!message.hasChunk()) {
                return new TransferResult(false, "expected a chunk message on this connection", state.outputPath);
            }
            FileChunk chunk = message.getChunk();

            if (!chunk.getTransferId().equals(state.transferId)) {
                return new TransferResult(false, "chunk transfer_id does not match this connection's transfer", state.outputPath);
            }

            if (state.isCommitted(chunk.getSequenceNumber())) {
                System.out.printf("Duplicate chunk ignored: transfer_id=%s seq=%d%n",
                        state.transferId, chunk.getSequenceNumber());
                continue; // already durable -- safe to skip, keeps resend idempotent
            }

            String actualChecksum = ChecksumUtils.sha256Hex(chunk.getData().toByteArray());
            if (!actualChecksum.equals(chunk.getChunkChecksum())) {
                System.out.printf("Checksum mismatch: transfer_id=%s seq=%d%n", state.transferId, chunk.getSequenceNumber());
                return new TransferResult(false,
                        "checksum mismatch on chunk " + chunk.getSequenceNumber(), state.outputPath);
            }

            long offset = (long) chunk.getSequenceNumber() * FileChunker.DEFAULT_CHUNK_SIZE;
            try (RandomAccessFile file = new RandomAccessFile(state.outputPath.toFile(), "rw")) {
                file.seek(offset);
                file.write(chunk.getData().toByteArray());
                file.getFD().sync(); // durability policy: fsync the data file per chunk (see README)
            }

            WalChunkCommitted committed = WalChunkCommitted.newBuilder()
                    .setTransferId(state.transferId)
                    .setSequenceNumber(chunk.getSequenceNumber())
                    .setFileOffset(offset)
                    .setChunkLength(chunk.getData().size())
                    .setChunkChecksum(chunk.getChunkChecksum())
                    .build();
            // WalWriter fsyncs per its own flush policy (EVERY_RECORD) before append() returns --
            // this line completing IS the durability boundary that write-before-ack refers to.
            wal.append(WalEntry.newBuilder().setChunkCommitted(committed).build().toByteArray());

            state.markCommitted(chunk.getSequenceNumber());
            System.out.printf("Chunk committed: transfer_id=%s seq=%d (%d/%d contiguous)%n",
                    state.transferId, chunk.getSequenceNumber(), state.highestContiguousCommitted() + 1, state.totalChunks);
        }

        if (state.isComplete()) {
            String actualFileChecksum = ChecksumUtils.sha256Hex(state.outputPath);
            boolean match = actualFileChecksum.equals(state.fileChecksum);
            return new TransferResult(match,
                    match ? "integrity check passed (" + state.filename + ")"
                          : "integrity check FAILED -- file checksum mismatch on " + state.filename,
                    state.outputPath);
        }

        // Connection closed before every chunk arrived -- not necessarily an
        // error. The client may reconnect and resume from here.
        return new TransferResult(false, String.format(
                "connection closed with transfer incomplete (%d/%d contiguous chunks committed)",
                state.highestContiguousCommitted() + 1, state.totalChunks), state.outputPath);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 2) {
            System.err.println("Usage: TransferServer <port> <outputDir>");
            System.exit(1);
        }
        TransferServer server = new TransferServer(Integer.parseInt(args[0]), Path.of(args[1]));
        server.start();
        System.out.println("Receiver listening on port " + args[0] + ", writing to " + args[1]);
        Thread.currentThread().join();
    }
}
