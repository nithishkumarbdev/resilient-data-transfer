package com.nithish.filetransfer.logging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * APP_LOG_PATH is read once, at class-load time, from the process
 * environment -- Java has no supported way to change its own already-
 * resolved environment variables mid-run, so the file-writing path is
 * verified separately via a real subprocess with the env var actually
 * set before the JVM starts (see the project's manual verification,
 * not a JUnit test, for that). What IS verifiable here, in-process: the
 * default (no APP_LOG_PATH) case never throws, and the line format is
 * correct.
 */
class AppLogTest {

    private final ByteArrayOutputStream capturedOut = new ByteArrayOutputStream();
    private PrintStream originalOut;

    @BeforeEach
    void captureStdout() {
        originalOut = System.out;
        System.setOut(new PrintStream(capturedOut));
    }

    @AfterEach
    void restoreStdout() {
        System.setOut(originalOut);
    }

    @Test
    @DisplayName("info/warn/error never throw, with or without extra key-value pairs")
    void neverThrows() {
        assertDoesNotThrow(() -> AppLog.info("test_event"));
        assertDoesNotThrow(() -> AppLog.info("test_event", "key", "value"));
        assertDoesNotThrow(() -> AppLog.warn("test_event", "key", "value", "key2", "value2"));
        assertDoesNotThrow(() -> AppLog.error("test_event", "odd_number_of_args"));
        assertDoesNotThrow(() -> AppLog.info("test_event", "key", null));
    }

    @Test
    @DisplayName("A log line includes the level, the event name, and every key=value pair")
    void lineFormatIsCorrect() {
        AppLog.info("transfer_started", "transfer_id", "abc-123", "filename", "report.pdf");
        String output = capturedOut.toString();

        assertTrue(output.contains("[INFO]"));
        assertTrue(output.contains("transfer_started"));
        assertTrue(output.contains("transfer_id=abc-123"));
        assertTrue(output.contains("filename=report.pdf"));
    }

    @Test
    @DisplayName("warn and error use their own level tags")
    void levelTagsAreCorrect() {
        AppLog.warn("checksum_mismatch", "seq", "4");
        AppLog.error("recovery_failed");
        String output = capturedOut.toString();

        assertTrue(output.contains("[WARN]"));
        assertTrue(output.contains("[ERROR]"));
    }
}
