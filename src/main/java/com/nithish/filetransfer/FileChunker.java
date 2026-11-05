package com.nithish.filetransfer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * We never read a whole file into memory in one Files.readAllBytes()
 * call — for Phase 1's correctness goal it wouldn't matter yet, but
 * chunking from the start keeps the design honest for when files are
 * bigger than RAM, and it's the same shape Phase 3's WAL will build on
 * (each chunk becomes a WAL record).
 */
public final class FileChunker {

    // 64 KB is a reasonable, easy-to-justify default: big enough to keep
    // per-message overhead low, small enough to keep memory and
    // retransmission cost (once we have retries) manageable.
    public static final int DEFAULT_CHUNK_SIZE = 64 * 1024;

    private FileChunker() {}

    public static List<byte[]> chunkFile(String path, int chunkSize) throws IOException {
        List<byte[]> chunks = new ArrayList<>();
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            long remaining = raf.length();
            while (remaining > 0) {
                int size = (int) Math.min(chunkSize, remaining);
                byte[] buffer = new byte[size];
                raf.readFully(buffer);
                chunks.add(buffer);
                remaining -= size;
            }
        }
        // A zero-byte file yields zero chunks — the metadata message
        // (total_chunks = 0) is what tells the receiver the transfer is
        // already "complete" once it arrives.
        return chunks;
    }
}
