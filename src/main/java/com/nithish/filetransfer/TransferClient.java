package com.nithish.filetransfer;

import com.google.protobuf.ByteString;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.ResumeRequest;
import com.nithish.filetransfer.proto.ResumeResponse;
import com.nithish.filetransfer.proto.TransferMessage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.UUID;

public class TransferClient {

    /**
     * Original Phase 1 behavior, unchanged: one connection, metadata then
     * every chunk, no resume logic. Still used by the plain-transfer tests.
     */
    public static String sendFile(String host, int port, Path filepath) throws IOException {
        if (!Files.exists(filepath)) {
            throw new IOException("File not found: " + filepath);
        }

        String transferId = UUID.randomUUID().toString();
        String filename = filepath.getFileName().toString();
        long totalSize = Files.size(filepath);
        String fileChecksum = ChecksumUtils.sha256Hex(filepath);
        List<byte[]> chunks = FileChunker.chunkFile(filepath.toString(), FileChunker.DEFAULT_CHUNK_SIZE);

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            sendMetadata(out, transferId, filename, totalSize, fileChecksum, chunks.size());
            for (int i = 0; i < chunks.size(); i++) {
                sendChunk(out, transferId, i, chunks.get(i));
            }
        }
        return transferId;
    }

    /**
     * Resume-aware send: if a connection fails partway through (e.g. the
     * server crashed), reconnects, asks the server where it actually got
     * to via a ResumeRequest, and continues from the next required chunk
     * instead of restarting from zero or assuming the client's own guess
     * about what was sent is what actually landed durably.
     *
     * chunkDelayMillis is a TEST-ONLY knob (0 in normal use) -- it exists
     * purely to make a mid-transfer crash reproducible on demand instead
     * of racing a real crash against however fast localhost happens to be.
     */
    public static TransferOutcome sendFileWithResume(String host, int port, Path filepath,
                                                       int maxRetries, long chunkDelayMillis) throws IOException {
        if (!Files.exists(filepath)) {
            throw new IOException("File not found: " + filepath);
        }

        String transferId = UUID.randomUUID().toString();
        String filename = filepath.getFileName().toString();
        long totalSize = Files.size(filepath);
        String fileChecksum = ChecksumUtils.sha256Hex(filepath);
        List<byte[]> chunks = FileChunker.chunkFile(filepath.toString(), FileChunker.DEFAULT_CHUNK_SIZE);
        int totalChunks = chunks.size();

        boolean metadataConfirmedSent = false;
        int nextChunkToSend = 0;
        IOException lastError = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try (Socket socket = new Socket(host, port);
                 DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                 DataInputStream in = new DataInputStream(socket.getInputStream())) {

                if (!metadataConfirmedSent) {
                    sendMetadata(out, transferId, filename, totalSize, fileChecksum, totalChunks);
                    metadataConfirmedSent = true;
                } else {
                    FramingUtils.writeFrame(out, TransferMessage.newBuilder()
                            .setResumeRequest(ResumeRequest.newBuilder().setTransferId(transferId).build())
                            .build().toByteArray());

                    byte[] frame = FramingUtils.readFrame(in);
                    if (frame == null) {
                        throw new IOException("Connection closed before resume response arrived");
                    }
                    ResumeResponse response = TransferMessage.parseFrom(frame).getResumeResponse();

                    if (response.getLastCommittedSequence() == -1) {
                        // Server has no record of this transfer (its own WAL never
                        // saw it) -- only safe option is to start over from scratch.
                        sendMetadata(out, transferId, filename, totalSize, fileChecksum, totalChunks);
                        nextChunkToSend = 0;
                    } else {
                        nextChunkToSend = response.getLastCommittedSequence() + 1;
                    }
                    System.out.println("Resumed: server says resume from chunk " + nextChunkToSend);
                }

                for (int i = nextChunkToSend; i < totalChunks; i++) {
                    sendChunk(out, transferId, i, chunks.get(i));
                    nextChunkToSend = i + 1;
                    if (chunkDelayMillis > 0) {
                        try { Thread.sleep(chunkDelayMillis); } catch (InterruptedException ignored) {}
                    }
                }

                return new TransferOutcome(transferId, true, attempt + 1);

            } catch (IOException e) {
                lastError = e;
                System.out.printf("Send attempt %d failed (%s) -- will reconnect and resume%n",
                        attempt + 1, e.getMessage());
                try {
                    Thread.sleep(300); // brief backoff so we're not hammering a dead/restarting server
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        throw new IOException("Transfer failed after " + (maxRetries + 1) + " attempt(s)", lastError);
    }

    public static final class TransferOutcome {
        public final String transferId;
        public final boolean success;
        public final int attemptsUsed;

        TransferOutcome(String transferId, boolean success, int attemptsUsed) {
            this.transferId = transferId;
            this.success = success;
            this.attemptsUsed = attemptsUsed;
        }
    }

    private static void sendMetadata(DataOutputStream out, String transferId, String filename,
                                      long totalSize, String fileChecksum, int totalChunks) throws IOException {
        FileMetadata metadata = FileMetadata.newBuilder()
                .setTransferId(transferId)
                .setFilename(filename)
                .setTotalSize(totalSize)
                .setTotalChunks(totalChunks)
                .setFileChecksum(fileChecksum)
                .build();
        FramingUtils.writeFrame(out, TransferMessage.newBuilder().setMetadata(metadata).build().toByteArray());
    }

    private static void sendChunk(DataOutputStream out, String transferId, int sequenceNumber, byte[] data) throws IOException {
        FileChunk chunk = FileChunk.newBuilder()
                .setTransferId(transferId)
                .setSequenceNumber(sequenceNumber)
                .setData(ByteString.copyFrom(data))
                .setChunkChecksum(ChecksumUtils.sha256Hex(data))
                .build();
        FramingUtils.writeFrame(out, TransferMessage.newBuilder().setChunk(chunk).build().toByteArray());
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.err.println("Usage: TransferClient <host> <port> <filepath>");
            System.exit(1);
        }
        Path path = Paths.get(args[2]);
        System.out.println("Sending " + path + " to " + args[0] + ":" + args[1] + " ...");
        String transferId = sendFile(args[0], Integer.parseInt(args[1]), path);
        System.out.println("Transfer complete. transfer_id=" + transferId);
    }
}
