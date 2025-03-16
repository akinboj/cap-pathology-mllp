package net.healthcare.digitaltwin.capmllp;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private final String failedMessagesPath;

    static {
        TOPIC_DESCRIPTIONS.put("AIP-34915", "ADT messages");
        TOPIC_DESCRIPTIONS.put("AIP-34728", "ORU messages");
        TOPIC_DESCRIPTIONS.put("ERROR-QUEUE", "Failed HL7 messages");
        TOPIC_DESCRIPTIONS.put("ERROR-QUEUE-ACK", "NACK for failed HL7 messages");
        TOPIC_DESCRIPTIONS.put("OUTAGE-QUEUE", "Messages stored when Kafka was unavailable");
    }

    public HL7Handler(CamelContext camel, String failedMessagesPath) {
        this.producer = camel.createProducerTemplate();
        this.failedMessagesPath = failedMessagesPath;
    }

    @Override
    public Message processMessage(Message request, Map<String, Object> metadata) throws ReceivingApplicationException {
        String hl7Msg;
        String patientId = "UNKNOWN";
        String msgType = "UNKNOWN";
        String topic;
        String desc;
        Message ack;

        try {
            hl7Msg = request.encode();
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
                ack = generateNegativeAck(request);  // NACK for unknown types
                sendToKafka(topic, hl7Msg, patientId);  // Log to ERROR-QUEUE
                return ack;  // Early return, skip ACK to Kafka
            }
            desc = TOPIC_DESCRIPTIONS.getOrDefault(topic, "unknown_topic");
        
            ack = request.generateACK();
            sendToKafka(topic, hl7Msg, patientId);
            sendAckToKafka(ack, topic, patientId, desc);
        } catch (HL7Exception | IOException e) {
            LOG.error("HL7 message processing failed", e);
            ack = generateNegativeAck(request);
            topic = "ERROR-QUEUE";
            desc = TOPIC_DESCRIPTIONS.get(topic);
            try {
                saveFailedMessage(request.encode());
            } catch (HL7Exception encodeException) {
                LOG.error("Failed to encode HL7 message for storage", encodeException);
            }
        }
        return ack;
    }

    private void sendToKafka(String topic, String message, String patientId) {
        try {
            producer.sendBodyAndHeader("kafka:" + topic, message, "kafka.KEY", patientId);
        } catch (Exception e) {
            LOG.error("Kafka send failure: Storing message locally.", e);
            saveFailedMessage(message);
        }
    }

    private void sendAckToKafka(Message ack, String topic, String patientId, String desc) throws IOException {
        try {
            String ackMsg = ack.encode();
            String ackTopic = topic + "-ACK";
            producer.sendBodyAndHeader("kafka:" + ackTopic, ackMsg, "kafka.KEY", patientId);
            LOG.info("Sent ACK to Kafka: topic={}, desc={}, patientId={}", ackTopic, desc, patientId);
        } catch (HL7Exception e) {
            LOG.error("Failed to encode ACK: {}", e.getMessage());
            throw new IOException("ACK encoding failed", e);
        }
    }

    private void saveFailedMessage(String message) {
        Path dirPath = Paths.get(failedMessagesPath);
        try {
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }
            String fileName = "failed_message_" + System.currentTimeMillis() + ".hl7";
            Path filePath = dirPath.resolve(fileName);
    
            try (FileWriter writer = new FileWriter(filePath.toFile(), true)) {
                writer.write(message);
                writer.write("\n");
            }
        } catch (IOException e) {
            LOG.error("Failed to store HL7 message", e);
        }
    }    

    private Message generateNegativeAck(Message request) {
        try {
            // Generate ACK from the original message to maintain MSH segment consistency
            Message ack = request.generateACK();
            
            // Get the MSA segment and set it to AE (Application Error)
            Terser terser = new Terser(ack);
            terser.set("/MSA-1", "AE");
            terser.set("/MSA-2", new Terser(request).get("/MSH-10")); // Set original message ID
            terser.set("/MSA-3", "Message processing error - Unsupported message type");
            
            return ack;
        } catch (HL7Exception | IOException e) { // Catch both
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
