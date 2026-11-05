package com.nithish.filetransfer.integrity;

import java.nio.file.Path;

/**
 * old_hash/new_hash are nullable by design: a FILE_CREATED event has no
 * old hash (nothing existed before), a FILE_DELETED event has no new
 * hash (nothing exists now). Never populated with file contents -- only
 * the digest and a short message, per the "don't include huge file
 * contents" requirement.
 */
public final class IntegrityEvent {
    public final long timestampMillis;
    public final Path path;
    public final IntegrityEventType type;
    public final String oldHash; // null for FILE_CREATED
    public final String newHash; // null for FILE_DELETED
    public final String message;

    public IntegrityEvent(long timestampMillis, Path path, IntegrityEventType type,
                           String oldHash, String newHash, String message) {
        this.timestampMillis = timestampMillis;
        this.path = path;
        this.type = type;
        this.oldHash = oldHash;
        this.newHash = newHash;
        this.message = message;
    }

    @Override
    public String toString() {
        return String.format("[%s] %s path=%s old=%s new=%s -- %s",
                timestampMillis, type, path, oldHash, newHash, message);
    }
}
