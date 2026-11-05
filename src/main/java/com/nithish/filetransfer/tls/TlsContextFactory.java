package com.nithish.filetransfer.tls;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;

/**
 * Every SSLContext here comes from standard javax.net.ssl APIs against a
 * real PKCS12 keystore/truststore. No custom TrustManager, no
 * HostnameVerifier that always returns true, no "trust everything"
 * shortcut anywhere in this file -- that's a deliberate constraint, not
 * an oversight: a trust-all TrustManager would make every rejection test
 * in TlsTransferTest pass for the wrong reason.
 */
public final class TlsContextFactory {

    /** Prefer TLSv1.3; this JDK supports it, and the spec asks for a modern default over hand-picked ciphers. */
    public static final String PROTOCOL = "TLSv1.3";

    private TlsContextFactory() {}

    /** Server-side context: presents this keystore's certificate/private key during the handshake. */
    public static SSLContext serverContext(Path keystorePath, char[] keystorePassword) throws GeneralSecurityException, IOException {
        KeyStore keyStore = loadKeyStore(keystorePath, keystorePassword);
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keystorePassword);

        SSLContext context = SSLContext.getInstance(PROTOCOL);
        // No TrustManagers on the server side -- this is server-authenticated TLS
        // (the client verifies the server), not mutual TLS. The server doesn't
        // need to validate a client certificate unless mTLS is explicitly added.
        context.init(kmf.getKeyManagers(), null, null);
        return context;
    }

    /** Client-side context: trusts exactly the certificates present in this truststore -- nothing else. */
    public static SSLContext clientContext(Path truststorePath, char[] truststorePassword) throws GeneralSecurityException, IOException {
        KeyStore trustStore = loadKeyStore(truststorePath, truststorePassword);
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore); // standard PKIX validation against exactly what's in this truststore

        SSLContext context = SSLContext.getInstance(PROTOCOL);
        context.init(null, tmf.getTrustManagers(), null);
        return context;
    }

    private static KeyStore loadKeyStore(Path path, char[] password) throws GeneralSecurityException, IOException {
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(path)) {
            keyStore.load(in, password);
        }
        return keyStore;
    }
}
