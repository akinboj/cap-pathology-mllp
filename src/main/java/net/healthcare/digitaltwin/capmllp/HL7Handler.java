package net.healthcare.digitaltwin.capmllp;

import ca.uhn.hl7v2.protocol.ReceivingApplication;
import ca.uhn.hl7v2.protocol.ReceivingApplicationException;
import ca.uhn.hl7v2.model.Message;
import ca.uhn.hl7v2.util.Terser;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class HL7Handler implements ReceivingApplication<Message> { // Fix interface
    private static final Logger LOG = LoggerFactory.getLogger(HL7Handler.class);
    private final KafkaProducer<String, String> producer;

    public HL7Handler(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    @Override
    public Message processMessage(Message request, Map<String, Object> metadata) 
            throws ReceivingApplicationException { // Fix exception
        try {
            String hl7Msg = request.encode();
            Terser terser = new Terser(request);
            String patientId = terser.get("/PID-3-1");

            LOG.info("Received HL7: {}", hl7Msg);
            producer.send(new ProducerRecord<>("cap_pathology_messages", patientId, hl7Msg));
            LOG.info("Sent to Kafka: patientId={}, event={}", patientId, hl7Msg);

            return request.generateACK();
        } catch (Exception e) {
            LOG.error("Error processing HL7 message", e);
            throw new ReceivingApplicationException("Processing failed", e); // Fix exception
        }
    }

    @Override
    public boolean canProcess(Message message) {
        return true;
    }
}