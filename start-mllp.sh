#!/bin/bash

set -e

# Ensure log directory for Kafka outage is created

mkdir -p "/var/log/${KUBERNETES_SERVICE_NAME}/outage-messages"

echo "Confirming if keystore/truststore files exist::"
echo ""

SSL_KEYSTORE_LOCATION="${MLLP_CERTS}/${KUBERNETES_SERVICE_NAME}.${KUBERNETES_NAMESPACE}.jks"
SSL_TRUSTSTORE_LOCATION="${MLLP_CERTS}/truststore.jks"

# Check if keystore/truststore exist
if [[ ! -f "$SSL_KEYSTORE_LOCATION" || ! -f "$SSL_TRUSTSTORE_LOCATION" ]]; then
    echo "❌ Keystore or Truststore is missing. Exiting..."
    exit 1
fi
echo "Keystore and Truststore exist"
echo ""

echo "Starting Capion Pathology MLLP Adapter::"
echo ""

exec java -Dlogback.configurationFile=/app/logback.xml -jar /app/cap-pathology-mllp-1.0.0-SNAPSHOT.jar