package com.nithish.filetransfer.integrity;

/**
 * Deliberately three types, not four. A separate INTEGRITY_VIOLATION type
 * would just be a renamed FILE_MODIFIED in this design: a modification
 * IS the violation signal once a file is in the baseline, so splitting
 * it into two types would mean choosing between them for the same event
 * for no real benefit. Keep it simple and explain the collapse if asked.
 */
public enum IntegrityEventType {
    FILE_CREATED,
    FILE_MODIFIED,
    FILE_DELETED
}
