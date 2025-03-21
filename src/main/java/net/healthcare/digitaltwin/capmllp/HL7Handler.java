package net.healthcare.digitaltwin.capmllp;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.hl7v2.HL7Exception;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.protocol.ReceivingApplication;
import ca.uhn.hl7v2.protocol.ReceivingApplicationException;
import ca.uhn.hl7v2.util.Terser;

public class HL7Handler implements ReceivingApplication<Message>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(HL7Handler.class);
    private static final Map<String, String> TOPIC_DESCRIPTIONS = new HashMap<>();

    private final ProducerTemplate producer;
    private final MessageStore messageStore;

    static {
        TOPIC_DESCRIPTIONS.put("AIP-34915", "ADT messages");
        TOPIC_DESCRIPTIONS.put("AIP-34728", "ORU messages");
        TOPIC_DESCRIPTIONS.put("ERROR-QUEUE", "Failed HL7 messages");
        TOPIC_DESCRIPTIONS.put("ERROR-QUEUE-ACK", "NACK for failed HL7 messages");
    }

    public HL7Handler(CamelContext camel, String basePath) {
        this.producer = camel.createProducerTemplate();
        this.messageStore = new MessageStore(basePath);
    }

    @Override
    public Message processMessage(Message request, Map<String, Object> metadata) throws ReceivingApplicationException {
        String hl7Msg = null;
        String patientId = "UNKNOWN";
        String msgType = "UNKNOWN";
        String topic;
        Message ack;

        try {
            hl7Msg = request.encode();
        } catch (HL7Exception e) {
            LOG.error("Failed to encode HL7 message initially", e);
            hl7Msg = metadata != null ? (String) metadata.get("RAW_MESSAGE") : "UNENCODED_MESSAGE";
        }
        
        try {
            Terser terser = new Terser(request);
            msgType = terser.get("/MSH-9-1");

            String[] possiblePaths = {
                "/PID-3-1",
                "/PATIENT_RESULT/PATIENT/PID-3-1",
                "/PATIENT/PID-3-1"
            };
            for (String path : possiblePaths) {
                try {
                    patientId = terser.get(path);
                    if (patientId != null && !patientId.isEmpty()) {
                        LOG.debug("Found patientId at {}: {}", path, patientId);
                        break;
                    }
                } catch (HL7Exception e) {
                    LOG.debug("No PID at {}: {}", path, e.getMessage());
                }
            }
            if ("UNKNOWN".equals(patientId)) {
                LOG.warn("No valid PID found in message");
            }

            if ("ORU".equals(msgType)) {
                topic = "AIP-34728";
            } else if ("ADT".equals(msgType)) {
                topic = "AIP-34915";
            } else {
                topic = "ERROR-QUEUE";
                LOG.warn("Unhandled message type: {} - routing to {}", msgType, topic);
                ack = generateNegativeAck(request);
                sendToKafka(topic, hl7Msg, patientId, ack);
                return ack;
            }

            ack = request.generateACK();
            sendToKafka(topic, hl7Msg, patientId, ack);
        } catch (HL7Exception | IOException e) {
            LOG.error("HL7 message processing failed", e);
            ack = generateNegativeAck(request);
            topic = "ERROR-QUEUE";
            sendToKafka(topic, hl7Msg, patientId, ack);
        }
        return ack;
    }

    private void sendToKafka(String topic, String message, String patientId, Message ack) {
        boolean kafkaSuccess = false;
        try {
            // Try sending to Kafka with fail-fast settings from KafkaConfig
            producer.sendBodyAndHeader("kafka:" + topic, message, "kafka.KEY", patientId);
            kafkaSuccess = true; // Only set if Kafka write succeeds
            LOG.debug("Sent message to Kafka: topic={}, patientId={}", topic, patientId);

            if (ack != null) {
                String ackMsg = ack.encode();
                String ackTopic = topic + "-ACK";
                producer.sendBodyAndHeader("kafka:" + ackTopic, ackMsg, "kafka.KEY", patientId);
                LOG.info("Sent ACK to Kafka: topic={}, patientId={}", ackTopic, patientId);
            }
        } catch (Exception e) {
            LOG.warn("Kafka write failed for topic={}, patientId={}: {}", topic, patientId, e.getMessage());
            // Fallback to local storage immediately on Kafka failure
            try {
                String ackMsg = ack != null ? ack.encode() : null;
                messageStore.save(topic, message, ackMsg);
                LOG.info("Stored message locally due to Kafka failure: topic={}, patientId={}", topic, patientId);
            } catch (HL7Exception ex) {
                LOG.error("Failed to encode ACK for storage: {}", ex.getMessage());
                throw new RuntimeException("Critical failure: Could not store message or ACK", ex);
            }
        }
        // If Kafka failed but storage succeeded, no exception thrown - message is safe
        if (!kafkaSuccess && !messageStoreFailed(topic, message)) {
            LOG.debug("Message safely stored after Kafka failure: patientId={}", patientId);
        }
    }

    private boolean messageStoreFailed(String topic, String message) {
        // Simple check - if save() didn't throw, assume success
        // Could enhance with explicit store verification if needed
        return false;
    }

    private Message generateNegativeAck(Message request) {
        try {
            Message ack = request.generateACK();
            Terser terser = new Terser(ack);
            terser.set("/MSA-1", "AE");
            terser.set("/MSA-2", new Terser(request).get("/MSH-10"));
            terser.set("/MSA-3", "Message processing error - Unsupported message type");
            return ack;
        } catch (HL7Exception | IOException e) {
            LOG.error("Failed to generate Negative ACK: {}", e.getMessage());
            return null;
        }
    }

    @Override
    public boolean canProcess(Message message) {
        return true;
    }

    @Override
    public void close() {
        producer.stop();
    }
}