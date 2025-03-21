package net.healthcare.digitaltwin.capmllp;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaConfiguration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaConfig {
    private static final String BROKER_ADDRESS = System.getenv("KAFKA_BOOTSTRAP_SERVERS") != null 
        ? System.getenv("KAFKA_BOOTSTRAP_SERVERS") 
        : "curis-data-broker.site-a.svc.cluster.local:31002";
    
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
        config.setPartitioner(null);

        // Fail-fast producer configuration
        Map<String, Object> producerProps = new HashMap<>();
        producerProps.put("max.block.ms", "100");
        producerProps.put("delivery.timeout.ms", "200");
        producerProps.put("request.timeout.ms", "150");
        producerProps.put("retries", "1");
        producerProps.put("compression.type", "snappy");
        producerProps.put("auto.create.topics.enable", "true");

        // Enable strong durability
        config.setRequestRequiredAcks("all");
        config.setEnableIdempotence(true);

        // Security: Mutual TLS
        Map<String, Object> additionalProps = new HashMap<>(getSslProperties());
        additionalProps.putAll(producerProps); // Combine with producer props
        config.setAdditionalProperties(additionalProps);

        // Serialization settings
        config.setKeySerializer("org.apache.kafka.common.serialization.StringSerializer");
        config.setValueSerializer("org.apache.kafka.common.serialization.StringSerializer");

        kafka.setConfiguration(config);
        return kafka;
    }
    
    public static void verifyTopicsExist() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_ADDRESS);
        props.putAll(getSslProperties()); // Use shared SSL config

        try (AdminClient admin = AdminClient.create(props)) {
            List<String> requiredTopics = Arrays.asList(
                "AIP-34915", "AIP-34915-ACK", 
                "AIP-34728", "AIP-34728-ACK",
                "ERROR-QUEUE", "ERROR-QUEUE-ACK"
            );

            Set<String> existingTopics = admin.listTopics().names().get(30, TimeUnit.SECONDS);
            List<String> missingTopics = requiredTopics.stream()
                .filter(topic -> !existingTopics.contains(topic))
                .collect(Collectors.toList());

            if (!missingTopics.isEmpty()) {
                List<NewTopic> newTopics = missingTopics.stream()
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .collect(Collectors.toList());
                admin.createTopics(newTopics).all().get(30, TimeUnit.SECONDS);
                System.out.println("Created missing topics: " + missingTopics);
            }
        } catch (Exception e) {
            throw new RuntimeException("Topic verification/creation failed: " + e.getMessage(), e);
        }
    }

    private static Map<String, Object> getSslProperties() {
        Map<String, Object> sslProps = new HashMap<>();
        sslProps.put("security.protocol", "SSL");
        sslProps.put("ssl.keystore.location", CERT_PATH + SERVICE_NAME + "." + NAMESPACE + ".jks");
        sslProps.put("ssl.keystore.password", System.getenv("KEYSTORE_PASSWORD"));
        sslProps.put("ssl.key.password", System.getenv("KEY_PASSWORD"));
        sslProps.put("ssl.truststore.location", CERT_PATH + TRUSTSTORE_FILE);
        sslProps.put("ssl.truststore.password", System.getenv("TRUSTSTORE_PASSWORD"));
        sslProps.put("ssl.keystore.type", "PKCS12");
        sslProps.put("ssl.truststore.type", "PKCS12");
        sslProps.put("ssl.endpoint.identification.algorithm", "HTTPS");
        sslProps.put("ssl.client.auth", "required");
        return sslProps;
    }
}