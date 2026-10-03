package com.nithish.filetransfer;

import com.google.protobuf.ByteString;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.TransferMessage;

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
     * Sends the file at {@code filepath} to host:port over a single TCP
     * connection: one metadata message, then one message per chunk, then
     * the connection is closed to signal "that's everything".
     * Returns the transfer_id that was generated, mostly so tests and
     * logs can correlate a send with its receive.
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
        int totalChunks = chunks.size();

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream())) {

            FileMetadata metadata = FileMetadata.newBuilder()
                    .setTransferId(transferId)
                    .setFilename(filename)
                    .setTotalSize(totalSize)
                    .setTotalChunks(totalChunks)
                    .setFileChecksum(fileChecksum)
                    .build();

            FramingUtils.writeFrame(out,
                    TransferMessage.newBuilder().setMetadata(metadata).build().toByteArray());

            for (int i = 0; i < chunks.size(); i++) {
                byte[] data = chunks.get(i);

                FileChunk chunk = FileChunk.newBuilder()
                        .setTransferId(transferId)
                        .setSequenceNumber(i)
                        .setData(ByteString.copyFrom(data))
                        .setChunkChecksum(ChecksumUtils.sha256Hex(data))
                        .build();

                FramingUtils.writeFrame(out,
                        TransferMessage.newBuilder().setChunk(chunk).build().toByteArray());
            }
        }
        return transferId;
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
