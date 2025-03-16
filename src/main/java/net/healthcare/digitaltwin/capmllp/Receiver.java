package net.healthcare.digitaltwin.capmllp;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.component.mllp.MllpConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;

public class Receiver {
	private static final String BROKER_ADDRESS = KafkaConfig.getBrokerAddress();
    private static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    private static final int MLLP_PORT = 2575;
    private static final int HEALTH_PORT = 8443;
    private static final String SERVICE_NAME = System.getenv("KUBERNETES_SERVICE_NAME");
    private static final String FAILED_MESSAGES_DIR = "/var/log/" + SERVICE_NAME + "/outage-messages/";
    private static final int KAFKA_HEALTH_CHECK_INTERVAL = 60;
    private static final int KAFKA_MAX_DOWN_TIME = 12 * 60 * 60;

    private static volatile boolean kafkaDown = false;
    private static volatile long kafkaDownStartTime = 0;
    private static CamelContext camel;
    private static HealthServer healthServer;

    public static void main(String[] args) throws Exception {
        startMLLPServer();
        startKafkaHealthCheck();
    }

    private static void startMLLPServer() throws Exception {
        camel = new DefaultCamelContext();
        camel.addComponent("kafka", KafkaConfig.createKafkaComponent());
        HL7Handler handler = new HL7Handler(camel, FAILED_MESSAGES_DIR);

        camel.addRoutes(createRouteBuilder(handler));

        // Start the health server for Kubernetes probes
        healthServer = new HealthServer(HEALTH_PORT);
        healthServer.start();

        LOG.info("Starting MLLP server on port {}", MLLP_PORT);
        camel.start();
    }

    private static RouteBuilder createRouteBuilder(HL7Handler handler) {
        return new RouteBuilder() {
            @Override
            public void configure() {
                HL7DataFormat hl7 = new HL7DataFormat();
                hl7.setValidate(false);

                from("mllp://0.0.0.0:" + MLLP_PORT + "?autoAck=false")
                    .routeId("mllp-receiver")
                    .process(exchange -> {
                        String raw = exchange.getIn().getBody(String.class);
                        LOG.debug("Raw input before fix: {}", raw);
                        String[] segments = raw.split("\r");
                        for (int i = 0; i < segments.length; i++) {
                            if (segments[i].startsWith("MSH")) {
                                String[] fields = segments[i].split("\\|");
                                if (fields.length > 11 && fields[11].contains("|")) {
                                    fields[11] = fields[11].split("\\|")[0]; // Handle various HL7 Versions
                                }
                                segments[i] = String.join("|", fields);
                                break;
                            }
                        }
                        String fixed = String.join("\r", segments);
                        LOG.debug("Fixed input: {}", fixed);
                        exchange.getIn().setBody(fixed);
                    })
                    .unmarshal(hl7)
                    .process(exchange -> {
                        Message parsedMessage = exchange.getIn().getBody(Message.class);
                        LOG.info("=**=> Received HL7 message: {}", parsedMessage.encode());

                        Map<String, Object> metadata = new HashMap<>();
                        metadata.put("RAW_MESSAGE", parsedMessage.encode());

                        Message ack = handler.processMessage(parsedMessage, metadata);

                        if (ack != null) {
                            try {
                                String ackString = ack.encode();
                                // Set both acknowledgment properties
                                exchange.setProperty(MllpConstants.MLLP_ACKNOWLEDGEMENT, ackString.getBytes());
                                exchange.setProperty(MllpConstants.MLLP_ACKNOWLEDGEMENT_STRING, ackString);
                                LOG.info("=**=> ACK sent: {}", ackString);
                            } catch (HL7Exception e) {
                                LOG.error("Failed to encode ACK", e);
                                throw new RuntimeCamelException("ACK encoding failed", e);
                            }
                        } else {
                            LOG.error("No ACK generated; ACK is null");
                        }
                    });
            }
        };
    }
    
    private static void startKafkaHealthCheck() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            boolean kafkaAvailable = isKafkaAvailable();
            if (!kafkaAvailable) {
                if (!kafkaDown) {
                    kafkaDown = true;
                    kafkaDownStartTime = System.currentTimeMillis();
                    LOG.warn("Kafka is DOWN. Started tracking downtime.");
                } else {
                    long downtime = (System.currentTimeMillis() - kafkaDownStartTime) / 1000;
                    if (downtime > KAFKA_MAX_DOWN_TIME) {
                        LOG.error("Kafka has been down for over 12 hours. Shutting down MLLP and Health servers.");
                        shutdownMLLPServer();
                    }
                }
            } else {
                if (kafkaDown) {
                    LOG.info("Kafka is back online. Restarting MLLP and Health servers.");
                    kafkaDown = false;
                    restartMLLPServer();
                    replayFailedMessages();
                }
            }
        }, 0, KAFKA_HEALTH_CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    private static boolean isKafkaAvailable() {
        try {
            String[] brokerParts = BROKER_ADDRESS.split(":");
            String host = brokerParts[0];
            int port = Integer.parseInt(brokerParts[1]);
            try (Socket socket = new Socket(host, port)) {
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

    private static void shutdownMLLPServer() {
        try {
            if (camel != null) {
                camel.stop();
                LOG.info("MLLP server shut down due to extended Kafka downtime.");
            }
            if (healthServer != null) {
                healthServer.stop();
                LOG.info("Health server shut down due to extended Kafka downtime.");
            }
        } catch (Exception e) {
            LOG.error("Error shutting down servers", e);
        }
    }

    private static void restartMLLPServer() {
        try {
            if (camel == null || !camel.isStarted()) {
                startMLLPServer();
            }
        } catch (Exception e) {
            LOG.error("Error restarting servers", e);
        }
    }

    private static void replayFailedMessages() {
        try {
            Path dirPath = Paths.get(FAILED_MESSAGES_DIR);
            if (!Files.exists(dirPath)) {
                return;
            }
    
            try (DirectoryStream<Path> files = Files.newDirectoryStream(dirPath)) {
                for (Path file : files) {
                    try {
                        String message = new String(Files.readAllBytes(file));
                        String topic = determineKafkaTopic(message);
                        LOG.info("Replaying stored message to Kafka: {}", topic);
    
                        try (CamelContext tempCamel = new DefaultCamelContext()) {
                            tempCamel.addComponent("kafka", KafkaConfig.createKafkaComponent());
                            tempCamel.start();
                            tempCamel.createProducerTemplate().sendBody("kafka:" + topic, message);
                        }
    
                        Files.delete(file);
                    } catch (Exception e) {
                        LOG.error("Failed to replay message from file: {}", file.getFileName(), e);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error("Failed to read stored messages for replay", e);
        }
    }        

    private static String determineKafkaTopic(String hl7Message) {
        if (hl7Message.contains("ADT")) {
            return "AIP-34915";
        } else if (hl7Message.contains("ORU")) {
            return "AIP-34728";
        }
        return "ERROR-QUEUE";
    }
}
