#!/bin/sh
# Generates a throwaway self-signed TLS certificate for LOCAL DEVELOPMENT
# ONLY, plus a keystore (server) and truststore (client) built from it.
#
# Never commit the .p12 files this script produces -- they're dev-only
# and regenerated on demand. Run this once before starting the server
# with TLS_ENABLED=true (see README).

set -e

OUT_DIR="${1:-./certs}"
PASSWORD="${TLS_DEV_PASSWORD:-changeit}"

mkdir -p "$OUT_DIR"

echo "Generating server keypair + self-signed cert (CN=localhost, SAN=localhost,127.0.0.1) ..."
keytool -genkeypair \
  -alias server \
  -keyalg RSA -keysize 2048 -validity 365 \
  -keystore "$OUT_DIR/server-keystore.p12" -storetype PKCS12 \
  -storepass "$PASSWORD" \
  -dname "CN=localhost, OU=dev, O=resilient-file-transfer, L=Local, ST=Local, C=US" \
  -ext "SAN=dns:localhost,ip:127.0.0.1"

echo "Exporting the server's public certificate ..."
keytool -exportcert \
  -alias server \
  -keystore "$OUT_DIR/server-keystore.p12" -storepass "$PASSWORD" \
  -file "$OUT_DIR/server.crt" -rfc

echo "Building the client truststore (trusts exactly this one certificate) ..."
keytool -importcert \
  -alias server \
  -file "$OUT_DIR/server.crt" \
  -keystore "$OUT_DIR/client-truststore.p12" -storetype PKCS12 \
  -storepass "$PASSWORD" -noprompt

echo ""
echo "Done. Files written to $OUT_DIR (all dev-only -- do not commit):"
echo "  server-keystore.p12    (server presents this during the TLS handshake)"
echo "  client-truststore.p12  (client trusts exactly the cert in here)"
echo "  server.crt             (the exported public cert, intermediate artifact)"
echo ""
echo "Start the server with TLS enabled via environment variables, e.g.:"
echo "  TLS_ENABLED=true TLS_PORT=9443 TLS_KEYSTORE_PATH=$OUT_DIR/server-keystore.p12 TLS_KEYSTORE_PASSWORD=$PASSWORD \\"
echo "    java -cp target/classes:... com.nithish.filetransfer.TransferServer 9000 /tmp/received 8080 9001"
