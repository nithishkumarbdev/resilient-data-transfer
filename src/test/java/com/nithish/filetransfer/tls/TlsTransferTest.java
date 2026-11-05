package com.nithish.filetransfer.tls;

import com.nithish.filetransfer.ChecksumUtils;
import com.nithish.filetransfer.FileChunker;
import com.nithish.filetransfer.TransferClient;
import com.nithish.filetransfer.TransferServer;
import com.nithish.filetransfer.api.ManagementApiServer;
import com.nithish.filetransfer.integrity.FileIntegrityMonitor;
import com.nithish.filetransfer.udp.UdpTransferClient;
import com.nithish.filetransfer.udp.UdpTransferServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Every certificate here is generated fresh, per test run, by the real
 * JDK keytool (see TestCertGenerator) -- there is no mocked SSLContext
 * and no trust-all TrustManager anywhere in this file or in the
 * production TlsContextFactory it exercises.
 */
class TlsTransferTest {

    private static Path certDir;
    private static Path validKeystore, validTruststore;
    private static Path wrongHostKeystore, wrongHostTruststore;
    private static Path untrustedTruststore;

    private TransferServer server;

    @BeforeAll
    static void generateCerts() throws Exception {
        certDir = Files.createTempDirectory("tls-test-certs");

        validKeystore = certDir.resolve("valid-server-keystore.p12");
        validTruststore = certDir.resolve("valid-client-truststore.p12");
        TestCertGenerator.generateKeystoreAndMatchingTruststore(certDir, "valid-server",
                "CN=localhost, OU=test", "dns:localhost,ip:127.0.0.1", validKeystore, validTruststore);

        wrongHostKeystore = certDir.resolve("wronghost-server-keystore.p12");
        wrongHostTruststore = certDir.resolve("wronghost-client-truststore.p12");
        TestCertGenerator.generateKeystoreAndMatchingTruststore(certDir, "wronghost-server",
                "CN=wronghost.example, OU=test", "dns:wronghost.example", wrongHostKeystore, wrongHostTruststore);

        untrustedTruststore = certDir.resolve("untrusted-truststore.p12");
        TestCertGenerator.generateEmptyTruststore(untrustedTruststore);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (server != null) server.stop();
    }

    private void startServerWithTls(Path outputDir, Path keystore) throws Exception {
        server = new TransferServer(0, outputDir);
        server.start();
        SSLContext serverContext = TlsContextFactory.serverContext(keystore, TestCertGenerator.PASSWORD.toCharArray());
        server.startTls(0, serverContext);
    }

    @Test
    @DisplayName("A normal TLS transfer completes and the reconstructed file's SHA-256 matches")
    void normalTlsTransfer() throws Exception {
        Path outputDir = Files.createTempDirectory("tls-normal-out");
        startServerWithTls(outputDir, validKeystore);

        Path input = Files.createTempFile("tls-normal-in", ".txt");
        Files.writeString(input, "a file sent over a real TLS-secured TCP connection");

        SSLContext clientContext = TlsContextFactory.clientContext(validTruststore, TestCertGenerator.PASSWORD.toCharArray());
        TransferClient.TransferOutcome outcome = TransferClient.sendFileWithResumeTls(
                "localhost", server.getTlsPort(), input, 2, 0, clientContext);

        assertTrue(outcome.success);
        Path output = outputDir.resolve(input.getFileName().toString());
        waitUntil(() -> sha256Matches(input, output));
    }

    @Test
    @DisplayName("Plain TCP keeps working unchanged on a server that also serves TLS")
    void plainTcpStillWorksAlongsideTls() throws Exception {
        Path outputDir = Files.createTempDirectory("tls-plain-out");
        startServerWithTls(outputDir, validKeystore);

        Path input = Files.createTempFile("tls-plain-in", ".txt");
        Files.writeString(input, "sent over plain TCP even though TLS is also enabled on this server");

        TransferClient.sendFile("localhost", server.getPort(), input);
        Path output = outputDir.resolve(input.getFileName().toString());
        waitUntil(() -> sha256Matches(input, output));
    }

    @Test
    @DisplayName("A multi-chunk file transfers correctly over TLS")
    void multiChunkTlsTransfer() throws Exception {
        Path outputDir = Files.createTempDirectory("tls-multi-out");
        startServerWithTls(outputDir, validKeystore);

        Path input = Files.createTempFile("tls-multi-in", ".bin");
        byte[] data = new byte[FileChunker.DEFAULT_CHUNK_SIZE * 3 + 500];
        new SecureRandom().nextBytes(data);
        Files.write(input, data);

        SSLContext clientContext = TlsContextFactory.clientContext(validTruststore, TestCertGenerator.PASSWORD.toCharArray());
        TransferClient.TransferOutcome outcome = TransferClient.sendFileWithResumeTls(
                "localhost", server.getTlsPort(), input, 5, 0, clientContext);

        assertTrue(outcome.success);
        Path output = outputDir.resolve(input.getFileName().toString());
        waitUntil(() -> Files.exists(output) && filesEqual(data, output));
        assertArrayEquals(data, Files.readAllBytes(output));
    }

    @Test
    @DisplayName("A TLS transfer interrupted by a server restart resumes correctly through the same WAL/resume path as plain TCP")
    void tlsTransferResumesAfterRestart() throws Exception {
        Path outputDir = Files.createTempDirectory("tls-resume-out");
        startServerWithTls(outputDir, validKeystore);
        int fixedTlsPort = server.getTlsPort(); // must stay stable across the restart below

        Path input = Files.createTempFile("tls-resume-in", ".bin");
        byte[] data = new byte[FileChunker.DEFAULT_CHUNK_SIZE * 4 + 500];
        new SecureRandom().nextBytes(data);
        Files.write(input, data);

        SSLContext clientContext = TlsContextFactory.clientContext(validTruststore, TestCertGenerator.PASSWORD.toCharArray());

        AtomicReference<TransferClient.TransferOutcome> outcomeRef = new AtomicReference<>();
        AtomicReference<Exception> errorRef = new AtomicReference<>();
        Thread clientThread = new Thread(() -> {
            try {
                outcomeRef.set(TransferClient.sendFileWithResumeTls("localhost", fixedTlsPort, input, 10, 150, clientContext));
            } catch (Exception e) {
                errorRef.set(e);
            }
        });
        clientThread.start();

        Thread.sleep(300);
        server.stop();

        server = new TransferServer(0, outputDir);
        server.start();
        SSLContext serverContext = TlsContextFactory.serverContext(validKeystore, TestCertGenerator.PASSWORD.toCharArray());
        server.startTls(fixedTlsPort, serverContext); // rebind the SAME port so the in-flight client's retries land here

        clientThread.join(20_000);

        assertNull(errorRef.get(), "Client thread threw: " + errorRef.get());
        assertNotNull(outcomeRef.get());
        assertTrue(outcomeRef.get().success);
        Path output = outputDir.resolve(input.getFileName().toString());
        assertArrayEquals(data, Files.readAllBytes(output));
    }

    @Test
    @DisplayName("A client with an untrusted (empty) truststore fails the TLS handshake; the server stays healthy afterward")
    void untrustedCertificateIsRejected() throws Exception {
        Path outputDir = Files.createTempDirectory("tls-untrusted-out");
        startServerWithTls(outputDir, validKeystore);

        Path input = Files.createTempFile("tls-untrusted-in", ".txt");
        Files.writeString(input, "should never arrive");

        SSLContext badClientContext = TlsContextFactory.clientContext(untrustedTruststore, TestCertGenerator.PASSWORD.toCharArray());

        IOException thrown = assertThrows(IOException.class, () ->
                TransferClient.sendFileWithResumeTls("localhost", server.getTlsPort(), input, 0, 0, badClientContext));
        assertTrue(causalChainContains(thrown, SSLException.class),
                "Expected an SSLException somewhere in the cause chain, got: " + thrown);

        // The server must still be healthy for a legitimate client afterward --
        // one client's bad handshake must not have taken it down.
        Path goodInput = Files.createTempFile("tls-after-untrusted-in", ".txt");
        Files.writeString(goodInput, "this one should succeed");
        SSLContext goodClientContext = TlsContextFactory.clientContext(validTruststore, TestCertGenerator.PASSWORD.toCharArray());
        TransferClient.TransferOutcome outcome = TransferClient.sendFileWithResumeTls(
                "localhost", server.getTlsPort(), goodInput, 2, 0, goodClientContext);
        assertTrue(outcome.success);
    }

    @Test
    @DisplayName("A certificate that doesn't match the connected hostname's identity is rejected by endpoint verification")
    void hostnameMismatchIsRejected() throws Exception {
        Path outputDir = Files.createTempDirectory("tls-hostmismatch-out");
        server = new TransferServer(0, outputDir);
        server.start();
        // Server presents a cert identifying "wronghost.example", not localhost.
        SSLContext serverContext = TlsContextFactory.serverContext(wrongHostKeystore, TestCertGenerator.PASSWORD.toCharArray());
        server.startTls(0, serverContext);

        Path input = Files.createTempFile("tls-hostmismatch-in", ".txt");
        Files.writeString(input, "should never arrive");

        // Client trusts this exact cert chain-wise (it's the only entry in its
        // truststore) but connects to "localhost" -- endpoint identification
        // must still reject it on the identity mismatch, not the trust chain.
        SSLContext clientContext = TlsContextFactory.clientContext(wrongHostTruststore, TestCertGenerator.PASSWORD.toCharArray());

        IOException thrown = assertThrows(IOException.class, () ->
                TransferClient.sendFileWithResumeTls("localhost", server.getTlsPort(), input, 0, 0, clientContext));
        assertTrue(causalChainContains(thrown, SSLException.class),
                "Expected an SSLException somewhere in the cause chain, got: " + thrown);
    }

    @Test
    @DisplayName("A wrong keystore password fails clearly at configuration time, not silently")
    void wrongKeystorePasswordFailsAtConfig() {
        // The JDK's KeyStore.load() reports a bad password as IOException, not
        // GeneralSecurityException -- confirmed by the actual message it throws
        // ("keystore password was incorrect"). Either way the point holds: this
        // fails clearly and immediately, with no silent fallback to an insecure mode.
        IOException thrown = assertThrows(IOException.class, () ->
                TlsContextFactory.serverContext(validKeystore, "totally-wrong-password".toCharArray()));
        assertTrue(thrown.getMessage() != null && thrown.getMessage().toLowerCase().contains("password"),
                "Expected a clear password-related error, got: " + thrown.getMessage());
    }

    @Test
    @DisplayName("A nonexistent keystore path fails clearly at configuration time")
    void missingKeystoreFileFailsAtConfig() {
        assertThrows(IOException.class, () ->
                TlsContextFactory.serverContext(Path.of("/nonexistent/path/keystore.p12"), TestCertGenerator.PASSWORD.toCharArray()));
    }

    @Test
    @DisplayName("The HTTP management API keeps working, and reflects the real transfer, while TLS is in use")
    void httpApiWorksAlongsideTls() throws Exception {
        Path outputDir = Files.createTempDirectory("tls-http-out");
        FileIntegrityMonitor fim = FileIntegrityMonitor.withExistingBaseline(outputDir,
                outputDir.resolve("integrity-baseline.txt"), outputDir.resolve("integrity-events.log"),
                Set.of("transfer.wal", "integrity-baseline.txt", "integrity-events.log"));
        fim.start();
        server = new TransferServer(0, outputDir, fim);
        server.start();
        SSLContext serverContext = TlsContextFactory.serverContext(validKeystore, TestCertGenerator.PASSWORD.toCharArray());
        server.startTls(0, serverContext);
        ManagementApiServer api = new ManagementApiServer(server, fim);
        api.start(0);

        try {
            Path input = Files.createTempFile("tls-http-in", ".txt");
            Files.writeString(input, "checked via HTTP while using TLS for the transfer");
            SSLContext clientContext = TlsContextFactory.clientContext(validTruststore, TestCertGenerator.PASSWORD.toCharArray());
            TransferClient.TransferOutcome outcome = TransferClient.sendFileWithResumeTls(
                    "localhost", server.getTlsPort(), input, 2, 0, clientContext);
            assertTrue(outcome.success);

            waitUntil(() -> {
                var state = server.getTransferState(outcome.transferId);
                return state != null && state.isComplete();
            });

            HttpClient http = HttpClient.newHttpClient();
            HttpResponse<String> health = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + api.getPort() + "/health")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, health.statusCode());

            HttpResponse<String> detail = http.send(
                    HttpRequest.newBuilder(URI.create("http://localhost:" + api.getPort() + "/transfers/" + outcome.transferId)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(200, detail.statusCode());
            assertTrue(detail.body().contains("\"status\":\"COMPLETED\""));
        } finally {
            api.stop();
            fim.stop();
        }
    }

    @Test
    @DisplayName("UDP still works on a server with TLS enabled (regression)")
    void udpStillWorksWithTlsEnabled() throws Exception {
        Path outputDir = Files.createTempDirectory("tls-udp-regression-out");
        startServerWithTls(outputDir, validKeystore);
        UdpTransferServer udpServer = new UdpTransferServer(0, server.getProcessor());
        udpServer.start();
        try {
            Path input = Files.createTempFile("tls-udp-in", ".txt");
            Files.writeString(input, "udp still works with tls enabled on the same server");
            UdpTransferClient.Outcome outcome = UdpTransferClient.sendFile("localhost", udpServer.getPort(), input, 500, 5);
            assertTrue(outcome.success);
        } finally {
            udpServer.stop();
        }
    }

    // --- helpers ---

    private static boolean causalChainContains(Throwable t, Class<?> type) {
        Throwable current = t;
        while (current != null) {
            if (type.isInstance(current)) return true;
            current = current.getCause();
        }
        return false;
    }

    private static boolean sha256Matches(Path a, Path b) {
        try {
            return Files.exists(b) && ChecksumUtils.sha256Hex(a).equals(ChecksumUtils.sha256Hex(b));
        } catch (IOException e) {
            return false;
        }
    }

    private static boolean filesEqual(byte[] expected, Path actual) {
        try {
            return Files.exists(actual) && java.util.Arrays.equals(expected, Files.readAllBytes(actual));
        } catch (IOException e) {
            return false;
        }
    }

    private void waitUntil(BooleanSupplier condition) throws InterruptedException {
        for (int i = 0; i < 1000 && !condition.getAsBoolean(); i++) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "Condition was never satisfied within timeout");
    }
}
