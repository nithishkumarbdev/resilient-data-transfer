package com.nithish.filetransfer.tls;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Generates real certificates via the JDK's own keytool (ProcessBuilder,
 * not a mocked TLS layer) so TlsTransferTest exercises actual X.509
 * validation. Nothing here is committed -- every keystore is generated
 * fresh into a temp directory per test run, exactly the workflow
 * generate-dev-certs.sh documents for a real developer.
 */
final class TestCertGenerator {

    static final String PASSWORD = "changeit";

    private TestCertGenerator() {}

    /** Generates a keypair+self-signed cert for the given identity, exports it, and builds a truststore that trusts exactly it. */
    static void generateKeystoreAndMatchingTruststore(Path dir, String alias, String dname, String san,
                                                        Path keystoreOut, Path truststoreOut) throws IOException, InterruptedException {
        Path certFile = dir.resolve(alias + ".crt");

        run("keytool", "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048", "-validity", "30",
                "-keystore", keystoreOut.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD,
                "-dname", dname, "-ext", "SAN=" + san);

        run("keytool", "-exportcert", "-alias", alias, "-keystore", keystoreOut.toString(),
                "-storepass", PASSWORD, "-file", certFile.toString(), "-rfc");

        run("keytool", "-importcert", "-alias", alias, "-file", certFile.toString(),
                "-keystore", truststoreOut.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD, "-noprompt");
    }

    /** An empty PKCS12 truststore -- trusts nothing, for the untrusted-certificate test. */
    static void generateEmptyTruststore(Path truststoreOut) throws IOException, InterruptedException {
        // keytool has no direct "create empty keystore" command; generating
        // then deleting an entry leaves a validly-formatted, empty store.
        run("keytool", "-genkeypair", "-alias", "throwaway", "-keyalg", "RSA", "-keysize", "2048", "-validity", "1",
                "-keystore", truststoreOut.toString(), "-storetype", "PKCS12", "-storepass", PASSWORD,
                "-dname", "CN=throwaway");
        run("keytool", "-delete", "-alias", "throwaway", "-keystore", truststoreOut.toString(), "-storepass", PASSWORD);
    }

    private static void run(String... command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(List.of(command)).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes());
        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        if (!finished || process.exitValue() != 0) {
            throw new IOException("keytool command failed (" + String.join(" ", command) + "):\n" + output);
        }
    }
}
