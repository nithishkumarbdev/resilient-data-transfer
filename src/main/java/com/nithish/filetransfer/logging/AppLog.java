package com.nithish.filetransfer.logging;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

/**
 * A CloudWatch Agent (or any log shipper) can only forward what's
 * actually written to a file -- stdout alone doesn't survive an EC2
 * reboot or get collected. This writes structured, key=value,
 * timestamped lines to BOTH stdout (so local terminal usage is
 * unchanged) and, when APP_LOG_PATH is set, to that file.
 *
 * Never throws. A log statement is not allowed to be the reason a
 * transfer fails -- if the file write fails (bad path, disk full,
 * permissions), that failure is reported to stderr once and logging
 * continues to stdout only.
 */
public final class AppLog {

    private static final Path LOG_FILE = resolveLogFile();
    private static volatile boolean warnedAboutFileFailure = false;

    private AppLog() {}

    public static void info(String event, String... kv) { write("INFO", event, kv); }
    public static void warn(String event, String... kv) { write("WARN", event, kv); }
    public static void error(String event, String... kv) { write("ERROR", event, kv); }

    private static synchronized void write(String level, String event, String... kv) {
        String line = format(level, event, kv);
        System.out.println(line);
        if (LOG_FILE != null) {
            try {
                Files.writeString(LOG_FILE, line + System.lineSeparator(), StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                if (!warnedAboutFileFailure) {
                    System.err.println("AppLog: failed to write to " + LOG_FILE + " (" + e.getMessage()
                            + ") -- continuing with stdout-only logging");
                    warnedAboutFileFailure = true;
                }
            }
        }
    }

    private static String format(String level, String event, String[] kv) {
        StringBuilder sb = new StringBuilder();
        sb.append(Instant.now()).append(" [").append(level).append("] ").append(event);
        for (int i = 0; i + 1 < kv.length; i += 2) {
            sb.append(' ').append(kv[i]).append('=').append(sanitize(kv[i + 1]));
        }
        return sb.toString();
    }

    private static String sanitize(String value) {
        return value == null ? "null" : value.replace('\n', ' ').replace('\r', ' ');
    }

    private static Path resolveLogFile() {
        String path = System.getenv("APP_LOG_PATH");
        if (path == null || path.isBlank()) {
            return null; // local default: stdout only
        }
        try {
            Path p = Path.of(path);
            if (p.getParent() != null) {
                Files.createDirectories(p.getParent());
            }
            return p;
        } catch (IOException e) {
            System.err.println("AppLog: could not prepare log path " + path + " (" + e.getMessage()
                    + ") -- continuing with stdout-only logging");
            return null;
        }
    }
}
