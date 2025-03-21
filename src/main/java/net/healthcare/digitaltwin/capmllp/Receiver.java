package net.healthcare.digitaltwin.capmllp;

import java.util.HashMap;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.hl7.HL7DataFormat;
import org.apache.camel.component.mllp.MllpConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.uhn.hl7v2.model.Message;

public class Receiver extends RouteBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(Receiver.class);
    private static final int MLLP_PORT = 2575;
    private final HL7Handler handler;

    public Receiver(CamelContext camel, String basePath) {
        this.handler = new HL7Handler(camel, basePath);
    }

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
                            fields[11] = fields[11].split("\\|")[0];
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
                    String ackString = ack.encode();
                    exchange.setProperty(MllpConstants.MLLP_ACKNOWLEDGEMENT, ackString.getBytes());
                    exchange.setProperty(MllpConstants.MLLP_ACKNOWLEDGEMENT_STRING, ackString);
                    LOG.info("=**=> ACK sent: {}", ackString);
                } else {
                    LOG.error("No ACK generated; ACK is null");
                }
            });
    }

    public HL7Handler getHandler() {
        return handler; // For ServerManager to close
    }
}