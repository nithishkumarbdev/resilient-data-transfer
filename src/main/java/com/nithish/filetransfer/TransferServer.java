package com.nithish.filetransfer;

import com.nithish.filetransfer.integrity.FileIntegrityMonitor;
import com.nithish.filetransfer.logging.AppLog;
import com.nithish.filetransfer.metrics.MetricsPublisher;
import com.nithish.filetransfer.proto.FileChunk;
import com.nithish.filetransfer.proto.FileMetadata;
import com.nithish.filetransfer.proto.ResumeRequest;
import com.nithish.filetransfer.proto.ResumeResponse;
import com.nithish.filetransfer.proto.TransferMessage;
import com.nithish.filetransfer.udp.UdpTransferServer;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The TCP transport -- and, optionally, TLS-secured TCP. All the actual
 * "is this chunk safe to keep" logic lives in TransferProcessor -- this
 * class is just framing (length-prefix) and connection handling on top
 * of it. TLS is deliberately just a different kind of ServerSocket:
 * SSLServerSocket.accept() returns an SSLSocket, which IS-A Socket, so
 * handleConnection() below is used completely unchanged for both plain
 * and TLS connections. Nothing about the application protocol
 * (Protobuf, chunking, WAL, resume) needed to know TLS exists.
 */
public class TransferServer {

    private final int port;
    private final TransferProcessor processor;
    private final MetricsPublisher metrics; // nullable-safe: only used for the ActiveConnections gauge here

    private ServerSocket serverSocket;
    private Thread acceptThread;
    private SSLServerSocket tlsServerSocket;
    private Thread tlsAcceptThread;
    private volatile TransferResult lastResult;
    private final AtomicInteger activeConnections = new AtomicInteger();

    public TransferServer(int port, Path outputDir) {
        this(port, outputDir, null);
    }

    public TransferServer(int port, Path outputDir, FileIntegrityMonitor fim) {
        this.port = port;
        this.processor = new TransferProcessor(outputDir, fim);
        this.metrics = null;
    }

    public TransferServer(int port, Path outputDir, FileIntegrityMonitor fim, MetricsPublisher metricsPublisher) {
        this.port = port;
        this.processor = new TransferProcessor(outputDir, fim, metricsPublisher);
        this.metrics = metricsPublisher;
    }

    public static class TransferResult {
        public final boolean success;
        public final String message;
        public final Path outputPath;

        TransferResult(boolean success, String message, Path outputPath) {
            this.success = success;
            this.message = message;
            this.outputPath = outputPath;
        }
    }

    public void start() throws IOException {
        processor.initialize();

        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true);
        serverSocket.bind(new InetSocketAddress(port));
        acceptThread = new Thread(() -> acceptLoop(serverSocket, false), "transfer-server-accept");
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    /**
     * Additive: plain TCP (start()) must already be running. Binds a
     * second, TLS-only listener on its own port -- TLS and plain TCP are
     * never multiplexed on the same port, which would require peeking at
     * the first bytes to guess the protocol. sslContext must have been
     * built from a real keystore (see TlsContextFactory) -- there is no
     * "trust everyone" mode.
     */
    public void startTls(int tlsPort, SSLContext sslContext) throws IOException {
        SSLServerSocketFactory factory = sslContext.getServerSocketFactory();
        tlsServerSocket = (SSLServerSocket) factory.createServerSocket();
        tlsServerSocket.setReuseAddress(true);
        tlsServerSocket.bind(new InetSocketAddress(tlsPort));
        tlsAcceptThread = new Thread(() -> acceptLoop(tlsServerSocket, true), "transfer-server-tls-accept");
        tlsAcceptThread.setDaemon(true);
        tlsAcceptThread.start();
        System.out.println("TLS receiver listening on port " + getTlsPort());
    }

    public void stop() throws IOException {
        if (serverSocket != null) {
            serverSocket.close();
        }
        if (tlsServerSocket != null) {
            tlsServerSocket.close();
        }
        if (acceptThread != null) {
            try { acceptThread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (tlsAcceptThread != null) {
            try { tlsAcceptThread.join(2000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        processor.close();
    }

    public TransferProcessor getProcessor() { return processor; }
    public TransferResult getLastResult() { return lastResult; }
    public TransferState getTransferState(String transferId) { return processor.getTransferState(transferId); }
    public Collection<TransferState> getAllTransferStates() { return processor.getAllTransferStates(); }
    public long getStartTimeMillis() { return processor.getStartTimeMillis(); }
    public int getActiveConnections() { return activeConnections.get(); }
    public int getRecoveredTransferCount() { return processor.getRecoveredTransferCount(); }
    public long getTransfersStarted() { return processor.getTransfersStarted(); }
    public long getTransfersCompleted() { return processor.getTransfersCompleted(); }
    public long getTransfersFailed() { return processor.getTransfersFailed(); }
    public long getBytesTransferred() { return processor.getBytesTransferred(); }
    public Path getOutputDir() { return processor.getOutputDir(); }
    public Path getWalPath() { return processor.getWalPath(); }

    /** The actual bound port -- if constructed with port 0 (ephemeral), this is what the OS actually assigned. */
    public int getPort() {
        return serverSocket.getLocalPort();
    }

    public int getTlsPort() {
        return tlsServerSocket.getLocalPort();
    }

    private void acceptLoop(ServerSocket socket, boolean isTls) {
        String label = isTls ? "TLS" : "TCP";
        while (socket != null && !socket.isClosed()) {
            try (Socket conn = socket.accept()) {
                if (isTls) {
                    try {
                        ((SSLSocket) conn).startHandshake(); // fail fast with a clear cause, before touching the protocol at all
                    } catch (IOException handshakeFailure) {
                        AppLog.warn("tls_handshake_failed", "remote", String.valueOf(conn.getRemoteSocketAddress()),
                                "reason", String.valueOf(handshakeFailure.getMessage()));
                        continue; // this one client's problem -- the server keeps accepting others
                    }
                }
                System.out.println(label + " connection from " + conn.getRemoteSocketAddress());
                int nowActive = activeConnections.incrementAndGet();
                if (metrics != null) metrics.recordValue("ActiveConnections", nowActive);
                try {
                    lastResult = handleConnection(conn);
                    logResult(lastResult);
                } finally {
                    int stillActive = activeConnections.decrementAndGet();
                    if (metrics != null) metrics.recordValue("ActiveConnections", stillActive);
                }
            } catch (IOException e) {
                if (socket.isClosed()) {
                    break;
                }
                System.err.println(label + " error handling connection: " + e.getMessage());
            }
        }
    }

    private void logResult(TransferResult result) {
        System.out.println((result.success ? "Transfer OK: " : "Transfer incomplete/failed: ") + result.message);
    }

    private TransferResult handleConnection(Socket socket) throws IOException {
        DataInputStream in = new DataInputStream(socket.getInputStream());
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());

        byte[] firstFrame = FramingUtils.readFrame(in);
        if (firstFrame == null) {
            return new TransferResult(false, "connection closed with no messages", null);
        }
        TransferMessage firstMessage = TransferMessage.parseFrom(firstFrame);

        TransferState state;
        if (firstMessage.hasMetadata()) {
            state = processor.registerOrGetTransfer(firstMessage.getMetadata(), "TCP");
        } else if (firstMessage.hasResumeRequest()) {
            state = handleResumeRequest(firstMessage.getResumeRequest(), out);
            if (state == null) {
                // Server has no record of this transfer_id (its own WAL never
                // saw it, e.g. it crashed before ever durably logging the
                // FileMetadata). The client's resume logic is designed to
                // respond to a -1 ResumeResponse by sending fresh FileMetadata
                // on THIS SAME connection -- so keep reading rather than
                // closing here, or that follow-up metadata has nowhere to land.
                byte[] nextFrame = FramingUtils.readFrame(in);
                if (nextFrame == null) {
                    return new TransferResult(false,
                            "resume requested for unknown transfer_id, and no follow-up metadata arrived", null);
                }
                TransferMessage nextMessage = TransferMessage.parseFrom(nextFrame);
                if (!nextMessage.hasMetadata()) {
                    return new TransferResult(false,
                            "expected fresh metadata after an unknown-transfer resume response", null);
                }
                state = processor.registerOrGetTransfer(nextMessage.getMetadata(), "TCP");
            }
        } else {
            return new TransferResult(false, "first message on a connection must be metadata or resume_request", null);
        }

        return receiveChunks(state, in);
    }

    private TransferState handleResumeRequest(ResumeRequest request, DataOutputStream out) throws IOException {
        TransferState state = processor.getTransferState(request.getTransferId());
        int lastCommitted = (state == null) ? -1 : state.highestContiguousCommitted();

        ResumeResponse response = ResumeResponse.newBuilder()
                .setTransferId(request.getTransferId())
                .setLastCommittedSequence(lastCommitted)
                .build();
        FramingUtils.writeFrame(out, TransferMessage.newBuilder().setResumeResponse(response).build().toByteArray());

        System.out.printf("Resume requested: transfer_id=%s -> last_committed_sequence=%d%n",
                request.getTransferId(), lastCommitted);
        return state;
    }

    private TransferResult receiveChunks(TransferState state, DataInputStream in) throws IOException {
        byte[] frame;
        while ((frame = FramingUtils.readFrame(in)) != null) {
            TransferMessage message = TransferMessage.parseFrom(frame);
            if (!message.hasChunk()) {
                return new TransferResult(false, "expected a chunk message on this connection", state.outputPath);
            }
            FileChunk chunk = message.getChunk();
            TransferProcessor.ChunkOutcome outcome = processor.processChunk(state, chunk);
            if (outcome == TransferProcessor.ChunkOutcome.TRANSFER_ID_MISMATCH) {
                return new TransferResult(false, "chunk transfer_id does not match this connection's transfer", state.outputPath);
            }
            if (outcome == TransferProcessor.ChunkOutcome.CHECKSUM_MISMATCH) {
                return new TransferResult(false, "checksum mismatch on chunk " + chunk.getSequenceNumber(), state.outputPath);
            }
            // COMMITTED and DUPLICATE both just continue reading the next frame.
        }

        TransferProcessor.CompletionResult completion = processor.finalizeIfComplete(state);
        if (completion.justCompleted) {
            return new TransferResult(completion.success, completion.message, state.outputPath);
        }

        return new TransferResult(false, String.format(
                "connection closed with transfer incomplete (%d/%d contiguous chunks committed)",
                state.highestContiguousCommitted() + 1, state.totalChunks), state.outputPath);
    }

    public static void main(String[] args) throws IOException, InterruptedException, java.security.GeneralSecurityException {
        if (args.length != 4) {
            System.err.println("Usage: TransferServer <tcpPort> <outputDir> <httpPort> <udpPort>");
            System.exit(1);
        }
        Path outputDir = Path.of(args[1]);
        com.nithish.filetransfer.metrics.MetricsPublisher metrics =
                com.nithish.filetransfer.metrics.MetricsPublisherFactory.create();

        java.util.Set<String> excluded = java.util.Set.of("transfer.wal", "integrity-baseline.txt", "integrity-events.log");
        FileIntegrityMonitor fim = FileIntegrityMonitor.withExistingBaseline(
                outputDir, outputDir.resolve("integrity-baseline.txt"), outputDir.resolve("integrity-events.log"), excluded);
        fim.setMetricsPublisher(metrics);
        fim.start();

        TransferServer server = new TransferServer(Integer.parseInt(args[0]), outputDir, fim, metrics);
        server.start();
        AppLog.info("server_start", "tcp_port", args[0], "output_dir", args[1]);

        // TLS is optional and configured via environment variables only --
        // never CLI args or hardcoded values, so a keystore password never
        // ends up in shell history or a log line.
        if ("true".equalsIgnoreCase(System.getenv("TLS_ENABLED"))) {
            int tlsPort = Integer.parseInt(System.getenv("TLS_PORT"));
            Path keystorePath = Path.of(System.getenv("TLS_KEYSTORE_PATH"));
            char[] keystorePassword = System.getenv("TLS_KEYSTORE_PASSWORD").toCharArray();
            SSLContext tlsContext = com.nithish.filetransfer.tls.TlsContextFactory.serverContext(keystorePath, keystorePassword);
            server.startTls(tlsPort, tlsContext);
        }

        UdpTransferServer udpServer = new UdpTransferServer(Integer.parseInt(args[3]), server.getProcessor());
        udpServer.start();

        com.nithish.filetransfer.api.ManagementApiServer api = new com.nithish.filetransfer.api.ManagementApiServer(server, fim);
        api.start(Integer.parseInt(args[2]));
        AppLog.info("api_start", "http_port", args[2]);

        Thread.currentThread().join();
    }
}
