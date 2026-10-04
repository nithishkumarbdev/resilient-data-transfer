package com.nithish.filetransfer.wal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class WalTest {

    private Path walFile;

    @BeforeEach
    void setUp() throws IOException {
        walFile = Files.createTempFile("test", ".wal");
        Files.delete(walFile); // WalWriter creates it fresh
    }

    @Test
    @DisplayName("Replaying a WAL file that was never created returns an empty list")
    void replayMissingFile_returnsEmpty() throws IOException {
        assertTrue(WalReader.replay(walFile).isEmpty());
    }

    @Test
    @DisplayName("Records written in order come back in the same order, intact")
    void appendAndReplay_roundTripsCorrectly() throws IOException {
        String[] payloads = {"first record", "second record", "third record"};

        try (WalWriter writer = new WalWriter(walFile, WalWriter.FlushPolicy.EVERY_RECORD)) {
            for (String p : payloads) {
                writer.append(p.getBytes(StandardCharsets.UTF_8));
            }
        }

        List<WalRecord> records = WalReader.replay(walFile);

        assertEquals(3, records.size());
        for (int i = 0; i < payloads.length; i++) {
            assertEquals(i, records.get(i).sequenceNumber);
            assertEquals(payloads[i], new String(records.get(i).payload, StandardCharsets.UTF_8));
        }
    }

    @Test
    @DisplayName("A length prefix with no complete record behind it (crash mid-write) is discarded cleanly")
    void replay_stopsAtTruncatedTail() throws IOException {
        try (WalWriter writer = new WalWriter(walFile, WalWriter.FlushPolicy.EVERY_RECORD)) {
            writer.append("good record 1".getBytes(StandardCharsets.UTF_8));
            writer.append("good record 2".getBytes(StandardCharsets.UTF_8));
        }

        // Simulate a crash mid-write: append a length prefix that promises
        // more bytes than actually follow it.
        try (RandomAccessFile raf = new RandomAccessFile(walFile.toFile(), "rw")) {
            raf.seek(raf.length());
            raf.writeInt(500); // claims a 500-byte record
            raf.write("but only this much was actually written".getBytes(StandardCharsets.UTF_8));
        }

        List<WalRecord> records = WalReader.replay(walFile);

        assertEquals(2, records.size(), "Only the two fully-written records should survive replay");
        assertEquals("good record 1", new String(records.get(0).payload, StandardCharsets.UTF_8));
        assertEquals("good record 2", new String(records.get(1).payload, StandardCharsets.UTF_8));
    }

    @Test
    @DisplayName("A record whose bytes were corrupted after being written is caught by the checksum")
    void replay_detectsCorruptedRecord() throws IOException {
        long secondRecordStartOffset;

        try (WalWriter writer = new WalWriter(walFile, WalWriter.FlushPolicy.EVERY_RECORD)) {
            writer.append("untouched record".getBytes(StandardCharsets.UTF_8));
            secondRecordStartOffset = fileSizeAfterOneRecord("untouched record");
            writer.append("this one gets corrupted".getBytes(StandardCharsets.UTF_8));
        }

        // Flip a byte inside the second record's payload, well after the
        // fact — same effect as a bad sector or bit rot, not a crash.
        try (RandomAccessFile raf = new RandomAccessFile(walFile.toFile(), "rw")) {
            long corruptOffset = secondRecordStartOffset + 4 /*len*/ + 8 /*seq*/ + 4 /*payloadLen*/ + 2;
            raf.seek(corruptOffset);
            raf.writeByte(raf.readByte() ^ 0xFF); // flip the byte at that offset
        }

        List<WalRecord> records = WalReader.replay(walFile);

        assertEquals(1, records.size(), "Corruption should stop replay right after the last good record");
        assertEquals("untouched record", new String(records.get(0).payload, StandardCharsets.UTF_8));
    }

    /** Size, in bytes, of a WAL file containing exactly one record with the given payload. */
    private long fileSizeAfterOneRecord(String payload) {
        int bodyLength = 8 + 4 + payload.getBytes(StandardCharsets.UTF_8).length;
        return 4 + bodyLength + 4 + 1; // length prefix + body + checksum + marker
    }
}
