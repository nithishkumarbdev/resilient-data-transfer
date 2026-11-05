package com.nithish.filetransfer;

import com.nithish.filetransfer.integrity.FileIntegrityMonitor;
import com.nithish.filetransfer.logging.AppLog;
import com.nithish.filetransfer.metrics.LoggingMetricsPublisher;
import com.nithish.filetransfer.metrics.MetricsPublisher;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.WalChunkCommitted;
import com.nithish.filetransfer.proto.WalEntry;
import com.nithish.filetransfer.proto.WalTransferStart;
import com.nithish.filetransfer.wal.WalWriter;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Everything about "safely accepting a chunk" that has nothing to do
 * with HOW the bytes arrived. TransferServer (TCP) and UdpTransferServer
 * both delegate here instead of each keeping their own copy of transfer
 * state, WAL writing, or FIM integration -- exactly the "TransferManager
 * underneath multiple Transports" shape this phase is demonstrating.
 *
 * One WalWriter, one transfers map, one FileIntegrityMonitor: a transfer
 * started over TCP and a transfer started over UDP are both just entries
 * in the same map, replayed from the same WAL on restart.
 */
public final class TransferProcessor {

    private final Path outputDir;
    private final Path walPath;
    private final FileIntegrityMonitor fim; // nullable
    private final MetricsPublisher metrics;

    private WalWriter wal;
    private Map<String, TransferState> transfers;
    private volatile int recoveredTransferCount;
    private final long startTimeMillis = System.currentTimeMillis();

    private final AtomicLong transfersStarted = new AtomicLong();
    private final AtomicLong transfersCompleted = new AtomicLong();
    private final AtomicLong transfersFailed = new AtomicLong();
    private final AtomicLong bytesTransferred = new AtomicLong();

    public enum ChunkOutcome { COMMITTED, DUPLICATE, CHECKSUM_MISMATCH, UNKNOWN_TRANSFER, TRANSFER_ID_MISMATCH }

    public TransferProcessor(Path outputDir, FileIntegrityMonitor fim) {
        this(outputDir, fim, new LoggingMetricsPublisher());
    }

    public TransferProcessor(Path outputDir, FileIntegrityMonitor fim, MetricsPublisher metrics) {
        this.outputDir = outputDir;
        this.walPath = outputDir.resolve("transfer.wal");
        this.fim = fim;
        this.metrics = metrics;
    }

    public void initialize() throws IOException {
        Files.createDirectories(outputDir);
        long recoveryStart = System.currentTimeMillis();
        transfers = new ConcurrentHashMap<>(RecoveryManager.recover(walPath, outputDir));
        long recoveryDurationMs = System.currentTimeMillis() - recoveryStart;
        recoveredTransferCount = transfers.size();
        if (!transfers.isEmpty()) {
            AppLog.info("wal_recovery", "recovered_transfers", String.valueOf(transfers.size()),
                    "duration_ms", String.valueOf(recoveryDurationMs));
            for (TransferState s : transfers.values()) {
                System.out.printf("  transfer_id=%s  transport=%s  %d/%d chunks contiguous-committed%n",
                        s.transferId, s.transport, s.highestContiguousCommitted() + 1, s.totalChunks);
                if (fim != null && !s.isComplete()) {
                    fim.markTransferActive(s.outputPath);
                }
            }
            metrics.incrementCounter("RecoveredTransfers", recoveredTransferCount);
            metrics.recordValue("WALRecoveryDurationMs", recoveryDurationMs);
        }
        wal = new WalWriter(walPath, WalWriter.FlushPolicy.EVERY_RECORD);
    }

    public void close() throws IOException {
        if (wal != null) {
            wal.close();
        }
        metrics.close();
    }

    /** Registers a new transfer, or returns the existing one if this metadata was already seen (idempotent resend). */
    public TransferState registerOrGetTransfer(FileMetadata metadata, String transport) throws IOException {
        TransferState existing = transfers.get(metadata.getTransferId());
        if (existing != null) {
            return existing;
        }

        Path outputPath = outputDir.resolve(metadata.getFilename());
        TransferState state = new TransferState(metadata.getTransferId(), metadata.getFilename(),
                metadata.getTotalSize(), metadata.getTotalChunks(), metadata.getFileChecksum(), outputPath,
                metadata.getChunkSize(), transport);

        try (RandomAccessFile file = new RandomAccessFile(outputPath.toFile(), "rw")) {
            file.setLength(metadata.getTotalSize());
        }
        if (fim != null) {
            fim.markTransferActive(outputPath);
        }

        WalTransferStart walStart = WalTransferStart.newBuilder()
                .setTransferId(metadata.getTransferId())
                .setFilename(metadata.getFilename())
                .setTotalSize(metadata.getTotalSize())
                .setTotalChunks(metadata.getTotalChunks())
                .setFileChecksum(metadata.getFileChecksum())
                .setChunkSize(metadata.getChunkSize())
                .setTransport(transport)
                .build();
        wal.append(WalEntry.newBuilder().setTransferStart(walStart).build().toByteArray());

        transfers.put(metadata.getTransferId(), state);
        transfersStarted.incrementAndGet();
        AppLog.info("transfer_started", "transfer_id", metadata.getTransferId(), "filename", metadata.getFilename(),
                "total_size", String.valueOf(metadata.getTotalSize()), "total_chunks", String.valueOf(metadata.getTotalChunks()),
                "transport", transport);
        metrics.incrementCounter("TransfersStarted", 1, Map.of("Transport", transport));
        return state;
    }

    /**
     * Validates and, if valid and new, durably commits one chunk: write
     * bytes at their offset, fsync, WAL-append, fsync, THEN mark
     * committed. This exact ordering is Phase 2's write-before-ack
     * durability boundary -- callers must not tell their client "ok"
     * before this method returns COMMITTED or DUPLICATE.
     */
    public ChunkOutcome processChunk(TransferState state, FileChunk chunk) throws IOException {
        if (!chunk.getTransferId().equals(state.transferId)) {
            return ChunkOutcome.TRANSFER_ID_MISMATCH;
        }
        if (state.isCommitted(chunk.getSequenceNumber())) {
            System.out.printf("Duplicate chunk ignored: transfer_id=%s seq=%d%n", state.transferId, chunk.getSequenceNumber());
            return ChunkOutcome.DUPLICATE;
        }

        String actualChecksum = ChecksumUtils.sha256Hex(chunk.getData().toByteArray());
        if (!actualChecksum.equals(chunk.getChunkChecksum())) {
            AppLog.warn("checksum_mismatch", "transfer_id", state.transferId, "sequence", String.valueOf(chunk.getSequenceNumber()));
            transfersFailed.incrementAndGet();
            metrics.incrementCounter("TransfersFailed", 1, Map.of("Transport", state.transport));
            return ChunkOutcome.CHECKSUM_MISMATCH;
        }

        long offset = (long) chunk.getSequenceNumber() * state.chunkSize;
        try (RandomAccessFile file = new RandomAccessFile(state.outputPath.toFile(), "rw")) {
            file.seek(offset);
            file.write(chunk.getData().toByteArray());
            file.getFD().sync();
        }

        WalChunkCommitted committed = WalChunkCommitted.newBuilder()
                .setTransferId(state.transferId)
                .setSequenceNumber(chunk.getSequenceNumber())
                .setFileOffset(offset)
                .setChunkLength(chunk.getData().size())
                .setChunkChecksum(chunk.getChunkChecksum())
                .build();
        wal.append(WalEntry.newBuilder().setChunkCommitted(committed).build().toByteArray());

        state.markCommitted(chunk.getSequenceNumber());
        bytesTransferred.addAndGet(chunk.getData().size());
        metrics.incrementCounter("BytesTransferred", chunk.getData().size(), Map.of("Transport", state.transport));
        System.out.printf("Chunk committed: transfer_id=%s seq=%d (%d/%d contiguous)%n",
                state.transferId, chunk.getSequenceNumber(), state.highestContiguousCommitted() + 1, state.totalChunks);
        return ChunkOutcome.COMMITTED;
    }

    /** Result of a completion check -- only meaningful the first time a transfer becomes complete. */
    public static final class CompletionResult {
        public final boolean justCompleted;
        public final boolean success;
        public final String message;

        CompletionResult(boolean justCompleted, boolean success, String message) {
            this.justCompleted = justCompleted;
            this.success = success;
            this.message = message;
        }
    }

    /** Call after any chunk commits -- checks whole-file integrity once all contiguous chunks are in, exactly once. */
    public CompletionResult finalizeIfComplete(TransferState state) throws IOException {
        if (!state.isComplete()) {
            return new CompletionResult(false, false, "incomplete");
        }
        String actualFileChecksum = ChecksumUtils.sha256Hex(state.outputPath);
        boolean match = actualFileChecksum.equals(state.fileChecksum);
        long durationMs = System.currentTimeMillis() - state.startTimeMillis; // real, measured -- not estimated
        Map<String, String> dims = Map.of("Transport", state.transport);

        if (match) {
            transfersCompleted.incrementAndGet();
            metrics.incrementCounter("TransfersCompleted", 1, dims);
            metrics.recordValue("TransferDurationMs", durationMs, dims);
            AppLog.info("transfer_completed", "transfer_id", state.transferId, "filename", state.filename,
                    "duration_ms", String.valueOf(durationMs), "transport", state.transport);
        } else {
            transfersFailed.incrementAndGet();
            metrics.incrementCounter("TransfersFailed", 1, dims);
            AppLog.warn("transfer_failed", "transfer_id", state.transferId, "filename", state.filename,
                    "reason", "checksum_mismatch", "transport", state.transport);
        }
        if (fim != null) {
            if (match) {
                fim.markTransferComplete(state.outputPath, actualFileChecksum, state.totalSize);
            } else {
                fim.unmarkTransferActive(state.outputPath);
            }
        }
        return new CompletionResult(true, match,
                match ? "integrity check passed (" + state.filename + ")"
                      : "integrity check FAILED -- file checksum mismatch on " + state.filename);
    }

    public TransferState getTransferState(String transferId) { return transfers.get(transferId); }
    public Collection<TransferState> getAllTransferStates() { return transfers.values(); }
    public long getStartTimeMillis() { return startTimeMillis; }
    public int getRecoveredTransferCount() { return recoveredTransferCount; }
    public long getTransfersStarted() { return transfersStarted.get(); }
    public long getTransfersCompleted() { return transfersCompleted.get(); }
    public long getTransfersFailed() { return transfersFailed.get(); }
    public long getBytesTransferred() { return bytesTransferred.get(); }
    public Path getOutputDir() { return outputDir; }
    public Path getWalPath() { return walPath; }
}
