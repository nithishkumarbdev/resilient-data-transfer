package com.nithish.filetransfer.api;

import com.nithish.filetransfer.TransferClient;
import com.nithish.filetransfer.TransferServer;
import com.nithish.filetransfer.integrity.FileIntegrityMonitor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

class ManagementApiServerTest {

    private Path outputDir;
    private Path inputDir;
    private FileIntegrityMonitor fim;
    private TransferServer server;
    private ManagementApiServer api;
    private final HttpClient http = HttpClient.newHttpClient();

    @BeforeEach
    void setUp() throws Exception {
        outputDir = Files.createTempDirectory("api-test-out");
        inputDir = Files.createTempDirectory("api-test-in");
        fim = FileIntegrityMonitor.withExistingBaseline(outputDir,
                outputDir.resolve("integrity-baseline.txt"), outputDir.resolve("integrity-events.log"),
                Set.of("transfer.wal", "integrity-baseline.txt", "integrity-events.log"));
        fim.start();
        server = new TransferServer(0, outputDir, fim); // ephemeral port -- fixed ports raced across rapid start/stop cycles
        server.start();
        api = new ManagementApiServer(server, fim);
        api.start(0); // ephemeral port, same reason
    }

    @AfterEach
    void tearDown() throws Exception {
        api.stop();
        server.stop();
        fim.stop();
    }

    @Test
    @DisplayName("GET /health returns 200 with a real, growing uptime")
    void health_returnsOk() throws Exception {
        HttpResponse<String> response = get("/health");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"status\":\"ok\""));
        assertTrue(response.body().contains("uptime_seconds"));
    }

    @Test
    @DisplayName("GET /server reflects real counters, not fake data")
    void server_reflectsRealState() throws Exception {
        HttpResponse<String> response = get("/server");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"transfers_started\":0"));
        assertTrue(response.body().contains("\"active_connections\""));
    }

    @Test
    @DisplayName("GET /transfers is empty before any transfer, then shows a real transfer after one runs")
    void transfers_showsRealTransferAfterOneRuns() throws Exception {
        HttpResponse<String> before = get("/transfers");
        assertEquals(200, before.statusCode());
        assertEquals("{\"status\":\"ok\",\"data\":[]}", before.body());

        Path input = inputDir.resolve("api-test.bin");
        byte[] data = new byte[120_000];
        new SecureRandom().nextBytes(data);
        Files.write(input, data);
        TransferClient.TransferOutcome outcome = TransferClient.sendFileWithResume("localhost", server.getPort(), input, 3, 0);
        assertTrue(outcome.success);

        // The client's socket closing (inside sendFileWithResume's
        // try-with-resources) doesn't mean the SERVER's accept thread has
        // finished processing the final frame yet -- poll for real
        // completion rather than assuming it's instant.
        waitUntil(() -> {
            var state = server.getTransferState(outcome.transferId);
            return state != null && state.isComplete();
        });

        HttpResponse<String> after = get("/transfers");
        assertEquals(200, after.statusCode());
        assertTrue(after.body().contains("api-test.bin"));
        assertTrue(after.body().contains("\"status\":\"COMPLETED\""));
        assertTrue(after.body().contains(outcome.transferId));
    }

    @Test
    @DisplayName("GET /transfers/{id} returns the real transfer detail; unknown id returns 404")
    void transferDetail_worksAndReturns404ForUnknown() throws Exception {
        Path input = inputDir.resolve("detail-test.bin");
        Files.write(input, "small file content for detail lookup".getBytes());
        TransferClient.TransferOutcome outcome = TransferClient.sendFileWithResume("localhost", server.getPort(), input, 3, 0);

        waitUntil(() -> {
            var state = server.getTransferState(outcome.transferId);
            return state != null && state.isComplete();
        });

        HttpResponse<String> found = get("/transfers/" + outcome.transferId);
        assertEquals(200, found.statusCode());
        assertTrue(found.body().contains("detail-test.bin"));

        HttpResponse<String> notFound = get("/transfers/does-not-exist");
        assertEquals(404, notFound.statusCode());
        assertTrue(notFound.body().contains("\"status\":\"error\""));
    }

    @Test
    @DisplayName("A path-traversal attempt in the transfer_id segment is rejected with 400, not resolved against the filesystem")
    void transferDetail_rejectsPathTraversal() throws Exception {
        HttpResponse<String> response = get("/transfers/..%2F..%2Fetc%2Fpasswd");
        assertEquals(400, response.statusCode());
    }

    @Test
    @DisplayName("GET /integrity/events reflects a real FIM event, and the type filter works")
    void integrityEvents_reflectsRealEvent() throws Exception {
        Path watched = outputDir.resolve("watched.txt");
        Files.writeString(watched, "original");
        // Reconciliation on next fim start would catch this, but the monitor
        // is already running -- write, then modify to generate a live event.
        waitUntil(() -> fim.getBaseline().contains(watched));

        Files.writeString(watched, "TAMPERED");
        waitUntil(() -> fim.getEventStore().loadAllQuiet().stream()
                .anyMatch(e -> e.type == com.nithish.filetransfer.integrity.IntegrityEventType.FILE_MODIFIED));

        HttpResponse<String> response = get("/integrity/events");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("FILE_MODIFIED"));

        HttpResponse<String> filtered = get("/integrity/events?type=FILE_DELETED");
        assertEquals(200, filtered.statusCode());
        assertEquals("{\"status\":\"ok\",\"data\":[]}", filtered.body());

        HttpResponse<String> badFilter = get("/integrity/events?type=NOT_A_REAL_TYPE");
        assertEquals(400, badFilter.statusCode());
    }

    @Test
    @DisplayName("GET /integrity/files reflects the real baseline")
    void integrityFiles_reflectsRealBaseline() throws Exception {
        Path watched = outputDir.resolve("tracked.txt");
        Files.writeString(watched, "content to be tracked");
        waitUntil(() -> fim.getBaseline().contains(watched));

        HttpResponse<String> response = get("/integrity/files");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("tracked.txt"));
        assertTrue(response.body().contains("sha256"));
    }

    @Test
    @DisplayName("GET /recovery reflects the real recovered-transfer count and WAL size")
    void recovery_reflectsRealState() throws Exception {
        HttpResponse<String> response = get("/recovery");
        assertEquals(200, response.statusCode());
        assertTrue(response.body().contains("\"recovered_transfer_count\":0"));
        assertTrue(response.body().contains("wal_size_bytes"));
    }

    // --- helpers ---

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + api.getPort() + path)).GET().build();
        return http.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private void waitUntil(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 150 && !condition.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "Condition was never satisfied within timeout");
    }
}
