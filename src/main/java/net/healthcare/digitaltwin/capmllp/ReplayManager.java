package net.healthcare.digitaltwin.capmllp;

import org.apache.camel.Exchange;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.GenericFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.Socket;

public class ReplayManager extends RouteBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ReplayManager.class);
    private static final String BROKER_ADDRESS = KafkaConfig.getBrokerAddress(); // Same as ServerManager
    private final String basePath;

    public ReplayManager(String basePath) {
        this.basePath = basePath;
    }

    private boolean isKafkaAvailable() {
        try {
            String[] brokerParts = BROKER_ADDRESS.split(":");
            String host = brokerParts[0];
            int port = Integer.parseInt(brokerParts[1]);
            try (Socket socket = new Socket(host, port)) {
                LOG.debug("Kafka check succeeded for replay: {}:{}", host, port);
                return true;
            }
        } catch (Exception e) {
            LOG.debug("Kafka unavailable for replay: {}", e.getMessage());
            return false;
        }
    }

    @Override
    public void configure() {
        // Global error handler
        errorHandler(defaultErrorHandler()
            .maximumRedeliveries(3)
            .redeliveryDelay(5000)
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .onExceptionOccurred(exchange -> {
                Exception cause = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                LOG.error("Failed to replay message: {}", cause.getMessage());
            }));

        // ADT Route
        from("file:" + basePath + "ADT/?noop=true&delay=5000")
            .routeId("ADT-replay-route")
            .autoStartup(false)
            .choice()
                .when(exchange -> isKafkaAvailable())
                    .process(exchange -> {
                        GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);
                        String content = exchange.getContext().getTypeConverter().convertTo(String.class, file.getBody());
                        exchange.getIn().setBody(content);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.info("Replayed message from ADT to AIP-34915: {}", fileName);
                    })
                    .to("kafka:AIP-34915")
                    .process(exchange -> {
                        String filePath = exchange.getIn().getHeader("CamelFileAbsolutePath", String.class);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        Path source = Paths.get(filePath);
                        Path target = Paths.get(basePath + "processed/" + fileName);
                        Files.createDirectories(target.getParent());
                        Files.move(source, target);
                        LOG.info("Moved to processed: {}", fileName);
                    })
                .otherwise()
                    .process(exchange -> {
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.warn("Kafka is down, skipping replay for ADT file: {}", fileName);
                    })
                    .stop()
            .end();

        // ADT-ACKS Route
        from("file:" + basePath + "ADT-ACKS/?noop=true&delay=5000")
            .routeId("ADT-ACKS-replay-route")
            .autoStartup(false)
            .choice()
                .when(exchange -> isKafkaAvailable())
                    .process(exchange -> {
                        GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);
                        String content = exchange.getContext().getTypeConverter().convertTo(String.class, file.getBody());
                        exchange.getIn().setBody(content);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.info("Replayed ACK from ADT-ACKS to AIP-34915-ACK: {}", fileName);
                    })
                    .to("kafka:AIP-34915-ACK")
                    .process(exchange -> {
                        String filePath = exchange.getIn().getHeader("CamelFileAbsolutePath", String.class);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        Path source = Paths.get(filePath);
                        Path target = Paths.get(basePath + "processed/" + fileName);
                        Files.createDirectories(target.getParent());
                        Files.move(source, target);
                        LOG.info("Moved to processed: {}", fileName);
                    })
                .otherwise()
                    .process(exchange -> {
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.warn("Kafka is down, skipping replay for ADT-ACKS file: {}", fileName);
                    })
                    .stop()
            .end();

        // ORU Route
        from("file:" + basePath + "ORU/?noop=true&delay=5000")
            .routeId("ORU-replay-route")
            .autoStartup(false)
            .choice()
                .when(exchange -> isKafkaAvailable())
                    .process(exchange -> {
                        GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);
                        String content = exchange.getContext().getTypeConverter().convertTo(String.class, file.getBody());
                        exchange.getIn().setBody(content);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.info("Replayed message from ORU to AIP-34728: {}", fileName);
                    })
                    .to("kafka:AIP-34728")
                    .process(exchange -> {
                        String filePath = exchange.getIn().getHeader("CamelFileAbsolutePath", String.class);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        Path source = Paths.get(filePath);
                        Path target = Paths.get(basePath + "processed/" + fileName);
                        Files.createDirectories(target.getParent());
                        Files.move(source, target);
                        LOG.info("Moved to processed: {}", fileName);
                    })
                .otherwise()
                    .process(exchange -> {
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.warn("Kafka is down, skipping replay for ORU file: {}", fileName);
                    })
                    .stop()
            .end();

        // ORU-ACKS Route
        from("file:" + basePath + "ORU-ACKS/?noop=true&delay=5000")
            .routeId("ORU-ACKS-replay-route")
            .autoStartup(false)
            .choice()
                .when(exchange -> isKafkaAvailable())
                    .process(exchange -> {
                        GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);
                        String content = exchange.getContext().getTypeConverter().convertTo(String.class, file.getBody());
                        exchange.getIn().setBody(content);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.info("Replayed ACK from ORU-ACKS to AIP-34728-ACK: {}", fileName);
                    })
                    .to("kafka:AIP-34728-ACK")
                    .process(exchange -> {
                        String filePath = exchange.getIn().getHeader("CamelFileAbsolutePath", String.class);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        Path source = Paths.get(filePath);
                        Path target = Paths.get(basePath + "processed/" + fileName);
                        Files.createDirectories(target.getParent());
                        Files.move(source, target);
                        LOG.info("Moved to processed: {}", fileName);
                    })
                .otherwise()
                    .process(exchange -> {
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.warn("Kafka is down, skipping replay for ORU-ACKS file: {}", fileName);
                    })
                    .stop()
            .end();

        // ERROR Route
        from("file:" + basePath + "ERROR/?noop=true&delay=5000")
            .routeId("ERROR-replay-route")
            .autoStartup(false)
            .choice()
                .when(exchange -> isKafkaAvailable())
                    .process(exchange -> {
                        GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);
                        String content = exchange.getContext().getTypeConverter().convertTo(String.class, file.getBody());
                        exchange.getIn().setBody(content);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.info("Replayed message from ERROR to ERROR-QUEUE: {}", fileName);
                    })
                    .to("kafka:ERROR-QUEUE")
                    .process(exchange -> {
                        String filePath = exchange.getIn().getHeader("CamelFileAbsolutePath", String.class);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        Path source = Paths.get(filePath);
                        Path target = Paths.get(basePath + "processed/" + fileName);
                        Files.createDirectories(target.getParent());
                        Files.move(source, target);
                        LOG.info("Moved to processed: {}", fileName);
                    })
                .otherwise()
                    .process(exchange -> {
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.warn("Kafka is down, skipping replay for ERROR file: {}", fileName);
                    })
                    .stop()
            .end();

        // ERROR-ACKS Route
        from("file:" + basePath + "ERROR-ACKS/?noop=true&delay=5000")
            .routeId("ERROR-ACKS-replay-route")
            .autoStartup(false)
            .choice()
                .when(exchange -> isKafkaAvailable())
                    .process(exchange -> {
                        GenericFile<?> file = exchange.getIn().getBody(GenericFile.class);
                        String content = exchange.getContext().getTypeConverter().convertTo(String.class, file.getBody());
                        exchange.getIn().setBody(content);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.info("Replayed ACK from ERROR-ACKS to ERROR-QUEUE-ACK: {}", fileName);
                    })
                    .to("kafka:ERROR-QUEUE-ACK")
                    .process(exchange -> {
                        String filePath = exchange.getIn().getHeader("CamelFileAbsolutePath", String.class);
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        Path source = Paths.get(filePath);
                        Path target = Paths.get(basePath + "processed/" + fileName);
                        Files.createDirectories(target.getParent());
                        Files.move(source, target);
                        LOG.info("Moved to processed: {}", fileName);
                    })
                .otherwise()
                    .process(exchange -> {
                        String fileName = exchange.getIn().getHeader("CamelFileNameOnly", String.class);
                        LOG.warn("Kafka is down, skipping replay for ERROR-ACKS file: {}", fileName);
                    })
                    .stop()
            .end();
    }
}