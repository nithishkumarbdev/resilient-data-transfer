package com.nithish.filetransfer;

import com.nithish.filetransfer.proto.WalChunkCommitted;
import com.nithish.filetransfer.proto.WalEntry;
import com.nithish.filetransfer.proto.WalTransferStart;
import com.nithish.filetransfer.wal.WalReader;
import com.nithish.filetransfer.wal.WalRecord;

import java.io.IOException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Startup recovery: replay every durable WAL entry in order and rebuild
 * the same TransferState a live server would have accumulated. This is
 * the whole recovery algorithm — there's no separate "repair" pass,
 * because replay naturally reconstructs exactly what was durably true
 * when the crash happened, no more and no less.
 */
public final class RecoveryManager {

    private RecoveryManager() {}

    public static Map<String, TransferState> recover(Path walFile, Path outputDir) throws IOException {
        Map<String, TransferState> transfers = new LinkedHashMap<>();
        List<WalRecord> records = WalReader.replay(walFile);

        for (WalRecord record : records) {
            WalEntry entry = WalEntry.parseFrom(record.payload);

            if (entry.hasTransferStart()) {
                WalTransferStart start = entry.getTransferStart();
                Path outputPath = outputDir.resolve(start.getFilename());
                transfers.put(start.getTransferId(), new TransferState(
                        start.getTransferId(), start.getFilename(), start.getTotalSize(),
                        start.getTotalChunks(), start.getFileChecksum(), outputPath));

            } else if (entry.hasChunkCommitted()) {
                WalChunkCommitted committed = entry.getChunkCommitted();
                TransferState state = transfers.get(committed.getTransferId());
                // A commit record with no matching transfer-start is not
                // supposed to happen given our write order, but recovery
                // should never crash on a pathological WAL — skip it.
                if (state != null) {
                    state.markCommitted(committed.getSequenceNumber());
                }
            }
        }
        return transfers;
    }
}
