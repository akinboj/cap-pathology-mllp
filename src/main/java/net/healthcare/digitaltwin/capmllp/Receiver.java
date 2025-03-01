package net.healthcare.digitaltwin.capmllp;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.hl7v2.app.SimpleServer;
import ca.uhn.hl7v2.llp.MinLowerLayerProtocol;
import ca.uhn.hl7v2.parser.PipeParser;

public class Receiver {
    private static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    private static final int MLLP_PORT = 8888;

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        PipeParser parser = new PipeParser();
        SimpleServer server = new SimpleServer(MLLP_PORT, new MinLowerLayerProtocol(), parser);
        
        server.registerApplication("*", "*", new HL7Handler(producer)); // Still works with ReceivingApplication

        LOG.info("Starting MLLP server on port {}", MLLP_PORT);
        server.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutting down MLLP server");
            server.stop();
            producer.close();
        }));

        Thread.currentThread().join();
    }
}