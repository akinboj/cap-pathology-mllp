package net.healthcare.digitaltwin.capmllp;

import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConfiguration;
import java.util.HashMap;
import java.util.Map;

public class KafkaConfig {
    private static final String BROKER_ADDRESS = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
        ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
        : "curis-data-broker.site-a.svc.cluster.local:31002";  // Default broker if env variable is missing
    
    public static String getBrokerAddress() {
        return BROKER_ADDRESS;
    }

    private static final String CERT_PATH = "/etc/mllp/secrets/";
    private static final String SERVICE_NAME = System.getenv("KUBERNETES_SERVICE_NAME");
    private static final String NAMESPACE = System.getenv("KUBERNETES_NAMESPACE");
    private static final String TRUSTSTORE_FILE = "truststore.jks";

    public static KafkaComponent createKafkaComponent() {
        KafkaComponent kafka = new KafkaComponent();
        KafkaConfiguration config = new KafkaConfiguration();

        config.setBrokers(BROKER_ADDRESS);

        // Enable strong durability
        config.setRequestRequiredAcks("all");  // Ensure message is stored in multiple replicas before acknowledging
        config.setRetries(5);  // Retry up to 5 times on transient failures
        config.setEnableIdempotence(true);  // Avoid duplicate messages on retries

        // Security: Mutual TLS
        config.setSecurityProtocol("SSL");
        config.setSslKeystoreLocation(CERT_PATH + SERVICE_NAME + "." + NAMESPACE + ".jks");
        config.setSslKeystorePassword(System.getenv("KEYSTORE_PASSWORD"));
        config.setSslKeyPassword(System.getenv("KEY_PASSWORD"));
        config.setSslTruststoreLocation(CERT_PATH + TRUSTSTORE_FILE);
        config.setSslTruststorePassword(System.getenv("TRUSTSTORE_PASSWORD"));
        config.setSslKeystoreType("PKCS12");
        config.setSslTruststoreType("PKCS12");

        // Additional SSL configurations
        Map<String, Object> additionalProps = new HashMap<>();
        additionalProps.put("ssl.endpoint.identification.algorithm", "HTTPS");
        additionalProps.put("ssl.client.auth", "required");
        config.setAdditionalProperties(additionalProps);

        // Serialization settings
        config.setKeySerializer("org.apache.kafka.common.serialization.StringSerializer");
        config.setValueSerializer("org.apache.kafka.common.serialization.StringSerializer");

        kafka.setConfiguration(config);
        return kafka;
    }
}
