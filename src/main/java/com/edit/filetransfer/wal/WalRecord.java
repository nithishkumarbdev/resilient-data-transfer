package com.nithish.filetransfer.wal;

/** One durably-committed entry recovered from the WAL during replay. */
public final class WalRecord {
    public final long sequenceNumber;
    public final byte[] payload;

    public WalRecord(long sequenceNumber, byte[] payload) {
        this.sequenceNumber = sequenceNumber;
        this.payload = payload;
    }
}
