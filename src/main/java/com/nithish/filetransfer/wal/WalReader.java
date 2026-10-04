package com.nithish.filetransfer.wal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Replays a WAL file written by WalWriter, in order, from the start.
 *
 * Recovery rule: stop at the first record that isn't fully present and
 * valid — don't try to skip past it and keep reading. Once one record's
 * length prefix or checksum can't be trusted, there's no reliable way to
 * know where the next record's header actually starts, so "resyncing" is
 * exactly how a single crash turns into silent corruption instead of a
 * clean recovery point. A truncated tail (the normal signature of a
 * kill -9 mid-write) and a corrupted middle are handled identically here,
 * on purpose — conservative, not clever.
 */
public final class WalReader {

    private WalReader() {}

    public static List<WalRecord> replay(Path walFile) throws IOException {
        List<WalRecord> records = new ArrayList<>();
        if (!Files.exists(walFile)) {
            return records;
        }

        try (RandomAccessFile file = new RandomAccessFile(walFile.toFile(), "r")) {
            long fileLength = file.length();

            while (file.getFilePointer() < fileLength) {
                long remaining = fileLength - file.getFilePointer();
                if (remaining < 4) {
                    break; // not even a full length prefix left — truncated tail
                }

                int recordLength = file.readInt();
                remaining = fileLength - file.getFilePointer();
                if (recordLength < 0 || remaining < recordLength) {
                    break; // declared length runs past what's actually on disk
                }

                byte[] record = new byte[recordLength];
                file.readFully(record);

                WalRecord parsed = tryParse(record);
                if (parsed == null) {
                    break; // checksum or commit marker didn't check out
                }
                records.add(parsed);
            }
        }
        return records;
    }

    private static WalRecord tryParse(byte[] record) throws IOException {
        int bodyLength = record.length - 4 /* checksum */ - 1 /* marker */;
        if (bodyLength < 12) { // need at least seq(8) + payloadLen(4)
            return null;
        }

        DataInputStream in = new DataInputStream(new ByteArrayInputStream(record));
        byte[] body = new byte[bodyLength];
        in.readFully(body);
        int storedChecksum = in.readInt();
        int marker = in.readUnsignedByte();

        CRC32 crc = new CRC32();
        crc.update(body);
        if (marker != 0x01 || (int) crc.getValue() != storedChecksum) {
            return null;
        }

        DataInputStream bodyIn = new DataInputStream(new ByteArrayInputStream(body));
        long seq = bodyIn.readLong();
        int payloadLen = bodyIn.readInt();
        if (payloadLen != bodyLength - 12) {
            return null; // internal inconsistency — treat as corrupt, don't guess
        }
        byte[] payload = new byte[payloadLen];
        bodyIn.readFully(payload);

        return new WalRecord(seq, payload);
    }
}
