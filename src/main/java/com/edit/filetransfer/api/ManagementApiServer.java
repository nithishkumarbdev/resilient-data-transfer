package com.nithish.filetransfer.api;

import com.nithish.filetransfer.TransferServer;
import com.nithish.filetransfer.TransferState;
import com.nithish.filetransfer.integrity.BaselineManager;
import com.nithish.filetransfer.integrity.FileIntegrityMonitor;
import com.nithish.filetransfer.integrity.IntegrityEvent;
import com.nithish.filetransfer.integrity.IntegrityEventType;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Read-only (mostly) management API, built on the JDK's own HttpServer --
 * no framework. This is visibility into the existing system, not a second
 * transfer protocol: every handler reads directly from the live
 * TransferServer / FileIntegrityMonitor state that the TCP path itself
 * uses. There is no separate copy of "API state" to keep in sync.
 *
 * Response envelope: {"status":"ok","data": ...} on success,
 * {"status":"error","message": "..."} on failure, with a real HTTP status
 * code (400/404) -- except /health, which matches the flat shape this
 * project's own spec document gave as its example.
 */
public final class ManagementApiServer {

    private final TransferServer transferServer;
    private final FileIntegrityMonitor fim; // nullable
    private HttpServer httpServer;

    public ManagementApiServer(TransferServer transferServer, FileIntegrityMonitor fim) {
        this.transferServer = transferServer;
        this.fim = fim;
    }

    public void start(int port) throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(port), 0);
        httpServer.createContext("/health", this::handleHealth);
        httpServer.createContext("/server", this::handleServer);
        httpServer.createContext("/transfers", this::handleTransfers);
        httpServer.createContext("/integrity/events", this::handleIntegrityEvents);
        httpServer.createContext("/integrity/files", this::handleIntegrityFiles);
        httpServer.createContext("/recovery", this::handleRecovery);
        httpServer.setExecutor(null);
        httpServer.start();
        System.out.println("Management API listening on port " + port);
    }

    public void stop() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    /** The actual bound port -- if start() was called with port 0 (ephemeral), this is what the OS assigned. */
    public int getPort() {
        return httpServer.getAddress().getPort();
    }

    // --- handlers ---

    private void handleHealth(HttpExchange exchange) throws IOException {
        long uptimeSeconds = (System.currentTimeMillis() - transferServer.getStartTimeMillis()) / 1000;
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("uptime_seconds", uptimeSeconds);
        writeJson(exchange, 200, body);
    }

    private void handleServer(HttpExchange exchange) throws IOException {
        long activeTransfers = transferServer.getAllTransferStates().stream().filter(s -> !s.isComplete()).count();
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("state", "running");
        data.put("active_connections", transferServer.getActiveConnections());
        data.put("active_transfers", activeTransfers);
        data.put("monitored_path", fim == null ? null : transferServer.getOutputDir().toString());
        data.put("transfers_started", transferServer.getTransfersStarted());
        data.put("transfers_completed", transferServer.getTransfersCompleted());
        data.put("transfers_failed", transferServer.getTransfersFailed());
        data.put("bytes_transferred", transferServer.getBytesTransferred());
        data.put("recovery_count", transferServer.getRecoveredTransferCount());
        data.put("integrity_events", fim == null ? 0 : quietEventCount());
        respondOk(exchange, data);
    }

    private void handleTransfers(HttpExchange exchange) throws IOException {
        String path = exchange.getRequestURI().getPath();
        String prefix = "/transfers";

        if (path.equals(prefix) || path.equals(prefix + "/")) {
            List<Object> list = new ArrayList<>();
            for (TransferState s : transferServer.getAllTransferStates()) {
                list.add(summarize(s));
            }
            respondOk(exchange, list);
            return;
        }

        String raw = path.substring(prefix.length() + 1);
        String transferId = validatePathSegment(raw, exchange);
        if (transferId == null) return; // already responded 400

        TransferState state = transferServer.getTransferState(transferId);
        if (state == null) {
            respondError(exchange, 404, "unknown transfer_id");
            return;
        }
        respondOk(exchange, summarize(state));
    }

    private void handleIntegrityEvents(HttpExchange exchange) throws IOException {
        if (fim == null) {
            respondOk(exchange, List.of());
            return;
        }
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        String typeFilter = query.get("type");
        IntegrityEventType wanted = null;
        if (typeFilter != null) {
            try {
                wanted = IntegrityEventType.valueOf(typeFilter.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                respondError(exchange, 400, "unknown type filter: " + typeFilter);
                return;
            }
        }

        List<IntegrityEvent> events;
        try {
            events = fim.getEventStore().loadAll();
        } catch (IOException e) {
            respondError(exchange, 500, "failed to read integrity event log");
            return;
        }

        List<Object> data = new ArrayList<>();
        for (IntegrityEvent e : events) {
            if (wanted != null && e.type != wanted) continue;
            data.add(eventJson(e));
        }
        respondOk(exchange, data);
    }

    private void handleIntegrityFiles(HttpExchange exchange) throws IOException {
        if (fim == null) {
            respondOk(exchange, List.of());
            return;
        }
        List<Object> data = new ArrayList<>();
        for (Map.Entry<Path, BaselineManager.Entry> e : fim.getBaseline().snapshot().entrySet()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("path", e.getKey().toString());
            item.put("sha256", e.getValue().sha256);
            item.put("size", e.getValue().size);
            data.add(item);
        }
        respondOk(exchange, data);
    }

    private void handleRecovery(HttpExchange exchange) throws IOException {
        Map<String, Object> data = new LinkedHashMap<>();
        data.put("recovered_transfer_count", transferServer.getRecoveredTransferCount());
        long walSize;
        try {
            walSize = Files.exists(transferServer.getWalPath()) ? Files.size(transferServer.getWalPath()) : 0;
        } catch (IOException e) {
            walSize = -1;
        }
        data.put("wal_size_bytes", walSize);
        respondOk(exchange, data);
    }

    // --- shaping ---

    private Object summarize(TransferState s) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("transfer_id", s.transferId);
        m.put("filename", s.filename);
        m.put("total_size", s.totalSize);
        m.put("total_chunks", s.totalChunks);
        m.put("committed_chunks", s.highestContiguousCommitted() + 1);
        m.put("status", status(s));
        return m;
    }

    /**
     * Only three states, all directly backed by real committed-chunk counts:
     * PENDING (nothing committed yet), TRANSFERRING (partial), COMPLETED
     * (all contiguous chunks committed). No FAILED/RECOVERING state --
     * this project doesn't durably track a distinct "this transfer failed"
     * flag per transfer_id, so reporting one would be inventing a state
     * not actually backed by data. A checksum failure surfaces via the
     * transfers_failed counter on /server instead, which IS real.
     */
    private String status(TransferState s) {
        int committed = s.highestContiguousCommitted() + 1;
        if (committed >= s.totalChunks) return "COMPLETED";
        if (committed > 0) return "TRANSFERRING";
        return "PENDING";
    }

    private Object eventJson(IntegrityEvent e) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("timestamp_millis", e.timestampMillis);
        m.put("type", e.type.name());
        m.put("path", e.path.toString());
        m.put("old_hash", e.oldHash);
        m.put("new_hash", e.newHash);
        m.put("message", e.message);
        return m;
    }

    private int quietEventCount() {
        try {
            return fim.getEventStore().loadAll().size();
        } catch (IOException e) {
            return -1;
        }
    }

    // --- security / validation (Part 8) ---

    /** Rejects an empty, path-traversal, or otherwise suspicious transfer_id segment. Responds 400 and returns null if invalid. */
    private String validatePathSegment(String raw, HttpExchange exchange) throws IOException {
        String decoded = URLDecoder.decode(raw, StandardCharsets.UTF_8);
        if (decoded.isBlank() || decoded.contains("..") || decoded.contains("/") || decoded.contains("\\")
                || decoded.length() > 200) {
            respondError(exchange, 400, "invalid transfer_id");
            return null;
        }
        return decoded;
    }

    private Map<String, String> parseQuery(String raw) {
        Map<String, String> result = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) return result;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) continue;
            String key = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            result.put(key, value);
        }
        return result;
    }

    // --- response writing ---

    private void respondOk(HttpExchange exchange, Object data) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "ok");
        body.put("data", data);
        writeJson(exchange, 200, body);
    }

    private void respondError(HttpExchange exchange, int statusCode, String message) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("status", "error");
        body.put("message", message);
        writeJson(exchange, statusCode, body);
    }

    private void writeJson(HttpExchange exchange, int statusCode, Object body) throws IOException {
        byte[] bytes = JsonUtil.toJson(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(statusCode, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
