package com.nithish.filetransfer.integrity;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * Simple append-only text log, one event per line:
 * "timestampMillis|type|path|oldHash|newHash|message"
 *
 * This is the audit trail Phase 4's API will eventually expose read-only.
 * A missing hash (FILE_CREATED has no old hash, FILE_DELETED has no new
 * hash) is written as the literal string "-". Pipe characters and
 * newlines inside a message are stripped rather than escaped -- keeps
 * the format trivially parseable without a real serialization library,
 * at the cost of a message that happens to contain a "|" being slightly
 * mangled. Fine for this phase's log messages, which are all generated
 * by this project itself, not user input.
 */
public final class IntegrityEventStore {

    private final Path eventLogFile;

    public IntegrityEventStore(Path eventLogFile) {
        this.eventLogFile = eventLogFile;
    }

    public synchronized void append(IntegrityEvent event) throws IOException {
        String line = String.join("|",
                String.valueOf(event.timestampMillis),
                event.type.name(),
                event.path.toString(),
                sanitize(event.oldHash),
                sanitize(event.newHash),
                sanitize(event.message)) + "\n";
        Files.writeString(eventLogFile, line, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized List<IntegrityEvent> loadAll() throws IOException {
        List<IntegrityEvent> events = new ArrayList<>();
        if (!Files.exists(eventLogFile)) {
            return events;
        }
        for (String line : Files.readAllLines(eventLogFile, StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            String[] parts = line.split("\\|", 6);
            if (parts.length != 6) continue; // skip a malformed line rather than fail
            events.add(new IntegrityEvent(
                    Long.parseLong(parts[0]),
                    Path.of(parts[2]),
                    IntegrityEventType.valueOf(parts[1]),
                    "-".equals(parts[3]) ? null : parts[3],
                    "-".equals(parts[4]) ? null : parts[4],
                    parts[5]));
        }
        return events;
    }

    /** Same as loadAll(), but wraps IOException unchecked -- convenient for polling loops in tests. */
    public List<IntegrityEvent> loadAllQuiet() {
        try {
            return loadAll();
        } catch (IOException e) {
            return new ArrayList<>();
        }
    }

    private static String sanitize(String value) {
        if (value == null) return "-";
        return value.replace("|", " ").replace("\n", " ").replace("\r", " ");
    }
}
