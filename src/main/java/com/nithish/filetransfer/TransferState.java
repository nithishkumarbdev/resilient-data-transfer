package com.nithish.filetransfer;

import java.nio.file.Path;
import java.util.TreeSet;

/**
 * In-memory state for one transfer, rebuilt at startup from the WAL by
 * RecoveryManager and kept live thereafter as new chunks commit.
 *
 * INTERVIEW-IMPORTANT: the resume point is the highest CONTIGUOUS
 * committed sequence number, not the highest one seen. If chunks
 * 0,1,2,4 are committed but 3 isn't, the safe resume point is "send
 * starting from 3" — trusting chunk 4 alone would leave a hole in the
 * file that nothing will ever fill in.
 */
public final class TransferState {

    public final String transferId;
    public final String filename;
    public final long totalSize;
    public final int totalChunks;
    public final String fileChecksum;
    public final Path outputPath;

    private final TreeSet<Integer> committedSequences = new TreeSet<>();

    public TransferState(String transferId, String filename, long totalSize,
                          int totalChunks, String fileChecksum, Path outputPath) {
        this.transferId = transferId;
        this.filename = filename;
        this.totalSize = totalSize;
        this.totalChunks = totalChunks;
        this.fileChecksum = fileChecksum;
        this.outputPath = outputPath;
    }

    public synchronized void markCommitted(int sequenceNumber) {
        committedSequences.add(sequenceNumber);
    }

    public synchronized boolean isCommitted(int sequenceNumber) {
        return committedSequences.contains(sequenceNumber);
    }

    /** Highest sequence number N such that 0..N are ALL committed. -1 if chunk 0 isn't committed yet. */
    public synchronized int highestContiguousCommitted() {
        int result = -1;
        for (int i = 0; i < totalChunks; i++) {
            if (committedSequences.contains(i)) {
                result = i;
            } else {
                break;
            }
        }
        return result;
    }

    public synchronized boolean isComplete() {
        return highestContiguousCommitted() == totalChunks - 1;
    }
}
