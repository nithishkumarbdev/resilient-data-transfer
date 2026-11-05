package com.nithish.filetransfer.wal;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;

/**
 * Append-only, single-writer durable log.
 *
 * On-disk record layout (all fields big-endian, written in this order):
 *
 *   [4 bytes]  record length L        — everything below this line
 *   [8 bytes]  sequence number
 *   [4 bytes]  payload length N
 *   [N bytes]  payload
 *   [4 bytes]  CRC32 checksum         — over (sequence number + payload length + payload)
 *   [1 byte]   commit marker          — must be 0x01 for the record to count as durable
 *
 * The whole point of this class is the ordering guarantee: append() must
 * return successfully — which, depending on flush policy, means the bytes
 * are actually on disk, not just sitting in a buffer — BEFORE the caller
 * is allowed to tell anyone else "this was received". That's what turns
 * a plain log file into a write-ahead log.
 */
public final class WalWriter implements AutoCloseable {

    public enum FlushPolicy {
        /** fsync after every record. Safest — survives an OS crash, not just a process crash — slowest. */
        EVERY_RECORD,
        /** Let the OS decide when to flush its page cache. Fast, but a few recent records can be
         *  lost on a hard crash even though append() already returned. Fine for Phase 3's process-kill
         *  test; not what you'd pick if the box itself might lose power. */
        OS_DEFAULT
    }

    private final RandomAccessFile file;
    private final FlushPolicy flushPolicy;
    private final AtomicLong nextSequenceNumber = new AtomicLong(0);

    public WalWriter(Path walFile, FlushPolicy flushPolicy) throws IOException {
        // Read the existing records first (before opening for write) so the
        // sequence counter continues correctly across a restart, instead of
        // colliding with numbers already on disk from a previous run.
        long existingCount = countExisting(walFile);

        this.file = new RandomAccessFile(walFile.toFile(), "rw");
        this.file.seek(this.file.length()); // resume appending after whatever's already there
        this.flushPolicy = flushPolicy;
        this.nextSequenceNumber.set(existingCount);
    }

    private static long countExisting(Path walFile) throws IOException {
        return WalReader.replay(walFile).size();
    }

    /** Appends payload durably (per flush policy) and returns the sequence number it was assigned. */
    public synchronized long append(byte[] payload) throws IOException {
        long seq = nextSequenceNumber.getAndIncrement();

        ByteArrayOutputStream bodyBuffer = new ByteArrayOutputStream();
        DataOutputStream body = new DataOutputStream(bodyBuffer);
        body.writeLong(seq);
        body.writeInt(payload.length);
        body.write(payload);
        byte[] bodyBytes = bodyBuffer.toByteArray();

        CRC32 crc = new CRC32();
        crc.update(bodyBytes);
        int checksum = (int) crc.getValue();

        int recordLength = bodyBytes.length + 4 /* checksum */ + 1 /* marker */;

        file.writeInt(recordLength);
        file.write(bodyBytes);
        file.writeInt(checksum);
        file.writeByte(0x01); // commit marker — written last, on purpose (see class javadoc)

        if (flushPolicy == FlushPolicy.EVERY_RECORD) {
            file.getFD().sync(); // force to physical disk, not just the OS page cache
        }

        return seq;
    }

    /** Sequence number that will be assigned to the *next* append() call. */
    public long nextSequenceNumber() {
        return nextSequenceNumber.get();
    }

    @Override
    public void close() throws IOException {
        file.close();
    }
}
