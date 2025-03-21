package net.healthcare.digitaltwin.capmllp;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.turbo.TurboFilter;
import ch.qos.logback.core.spi.FilterReply;

public class KafkaThrottleFilter extends TurboFilter {
    private static final long THROTTLE_DURATION_MS = TimeUnit.MINUTES.toMillis(5);
    private static final AtomicLong lastLogTime = new AtomicLong(0);

    @Override
    public FilterReply decide(
            org.slf4j.Marker marker, ch.qos.logback.classic.Logger logger,
            Level level, String format, Object[] params, Throwable t) {

        if (logger.getName().contains("org.apache.kafka.clients.NetworkClient") &&
            format != null && (format.contains("Broker may not be available") || format.contains("disconnected"))) {

            long now = System.currentTimeMillis();
            long lastLogged = lastLogTime.get();

            if (now - lastLogged < THROTTLE_DURATION_MS) {
                return FilterReply.DENY; // Suppress duplicate logs within 5-minute window
            }

            lastLogTime.set(now); // Update last log timestamp
        }

        return FilterReply.NEUTRAL; // Allow other logs
    }
}
