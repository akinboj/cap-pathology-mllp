package net.healthcare.digitaltwin.capmllp;

import org.apache.camel.CamelContext;
import org.apache.camel.impl.DefaultCamelContext;
import java.net.Socket;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerManager {
    private static final String BROKER_ADDRESS = KafkaConfig.getBrokerAddress();
    private static final Logger LOG = LoggerFactory.getLogger(ServerManager.class);
    private static final int HEALTH_PORT = 8443;
    private static final String SERVICE_NAME = System.getenv("KUBERNETES_SERVICE_NAME");
    private static final String BASE_PATH = "/var/log/" + SERVICE_NAME + "/outage-messages/";
    private static final int KAFKA_HEALTH_CHECK_INTERVAL = 60; // Seconds
    private static final int KAFKA_MAX_DOWN_TIME = 12 * 60 * 60; // Seconds (12 hours)
    private static final String MLLP_ROUTE_ID = "mllp-receiver"; // Matches Receiver

    // Route IDs from ReplayManager (must match exactly)
    private static final String[] REPLAY_ROUTE_IDS = {
        "ADT-replay-route",
        "ADT-ACKS-replay-route",
        "ORU-replay-route",
        "ORU-ACKS-replay-route",
        "ERROR-replay-route",
        "ERROR-ACKS-replay-route"
    };

    private static volatile boolean kafkaDown = false; // Start assuming UP
    private static volatile long kafkaDownStartTime = 0;
    
    private CamelContext camel;
    private HealthServer healthServer;
    private HL7Handler handler;
    private ScheduledExecutorService healthCheckScheduler;
    private boolean mllpStopped = false; // Track MLLP state

    public static void main(String[] args) throws Exception {
        ServerManager manager = new ServerManager();
        manager.start();
    }

    public void start() throws Exception {
        // Ensure topics exist before starting anything
        try {
            KafkaConfig.verifyTopicsExist();
            LOG.info("Verified/created Kafka topics on startup");
        } catch (Exception e) {
            LOG.error("Failed to verify/create Kafka topics on startup", e);
            throw e; // Optional: fail fast if critical
        }
        
        camel = new DefaultCamelContext();
        camel.addComponent("kafka", KafkaConfig.createKafkaComponent());

        Receiver receiver = new Receiver(camel, BASE_PATH);
        this.handler = receiver.getHandler();
        camel.addRoutes(receiver);

        ReplayManager replayManager = new ReplayManager(BASE_PATH);
        camel.addRoutes(replayManager);

        healthServer = new HealthServer(HEALTH_PORT);
        healthServer.start();

        LOG.info("Starting MLLP server and replay routes");
        camel.start();
        
        // Initial health check and route state setup
        initializeSystemState();
        
        startKafkaHealthCheck();
    }

    private void initializeSystemState() {
        // Run immediate health check before any route management
        boolean initialKafkaState = isKafkaAvailable();
        kafkaDown = !initialKafkaState;
        
        if (initialKafkaState) {
            LOG.info("Initial Kafka check: UP - Starting replay routes");
            startReplayRoutes();
        } else {
            LOG.warn("Initial Kafka check: DOWN - Keeping replay routes stopped");
            kafkaDownStartTime = System.currentTimeMillis();
            stopReplayRoutes();
        }
    }

    private void startKafkaHealthCheck() {
        healthCheckScheduler = Executors.newScheduledThreadPool(1);
        healthCheckScheduler.scheduleAtFixedRate(() -> {
            try {
                boolean kafkaAvailable = isKafkaAvailable();
                
                if (kafkaAvailable) {
                    handleKafkaRecovery();
                } else {
                    handleKafkaDowntime();
                }
            } catch (Exception e) {
                LOG.error("Health check failed", e);
            }
        }, 0, KAFKA_HEALTH_CHECK_INTERVAL, TimeUnit.SECONDS);
    }

    private void handleKafkaRecovery() {
        if (kafkaDown) {
            long downtimeSeconds = (System.currentTimeMillis() - kafkaDownStartTime) / 1000;
            String downtimeFormatted = formatDowntime(downtimeSeconds);
            LOG.info("Kafka recovered after {}", downtimeFormatted);
            kafkaDown = false;
            startReplayRoutes();
            if (mllpStopped) {
                try {
                    camel.getRouteController().startRoute(MLLP_ROUTE_ID);
                    LOG.info("MLLP route {} resumed accepting connections", MLLP_ROUTE_ID);
                    mllpStopped = false;
                } catch (Exception e) {
                    LOG.error("Failed to restart MLLP route {}: {}", MLLP_ROUTE_ID, e.getMessage());
                }
            }
        }
    }

    private void handleKafkaDowntime() {
        if (!kafkaDown) {
            // First failure detection
            kafkaDown = true;
            kafkaDownStartTime = System.currentTimeMillis();
            LOG.warn("Kafka connection lost. Stopping replay routes.");
            stopReplayRoutes();
        } else {
            long downtimeSeconds = (System.currentTimeMillis() - kafkaDownStartTime) / 1000;
            String downtimeFormatted = formatDowntime(downtimeSeconds);
            LOG.warn("Kafka still down ({})", downtimeFormatted);
            
            if (downtimeSeconds > KAFKA_MAX_DOWN_TIME && !mllpStopped) {
                LOG.error("CRITICAL DOWNTIME EXCEEDED {} hours. Refusing MLLP connections.", 
                    KAFKA_MAX_DOWN_TIME / 3600);
                try {
                    camel.getRouteController().stopRoute(MLLP_ROUTE_ID);
                    LOG.info("MLLP route {} stopped to refuse connections", MLLP_ROUTE_ID);
                    mllpStopped = true;
                } catch (Exception e) {
                    LOG.error("Failed to stop MLLP route {}: {}", MLLP_ROUTE_ID, e.getMessage());
                }
            }
        }
    }

    private boolean isKafkaAvailable() {
        try {
            String[] brokerParts = BROKER_ADDRESS.split(":");
            String host = brokerParts[0];
            int port = Integer.parseInt(brokerParts[1]);
            
            try (Socket socket = new Socket(host, port)) {
                LOG.debug("Kafka health check succeeded to {}:{}", host, port);
                return true;
            }
        } catch (Exception e) {
            LOG.warn("Kafka health check failed: {}", e.getMessage());
            return false;
        }
    }
    
    private String formatDowntime(long seconds) {
        long minutes = seconds / 60;
        if (minutes < 60) {
            return minutes + " minute" + (minutes == 1 ? "" : "s");
        } else {
            long hours = minutes / 60;
            long remainingMinutes = minutes % 60;
            return hours + " hour" + (hours == 1 ? "" : "s") + " " + 
                   remainingMinutes + " minute" + (remainingMinutes == 1 ? "" : "s");
        }
    }

    private void startReplayRoutes() {
        LOG.info("Starting {} replay routes", REPLAY_ROUTE_IDS.length);
        for (String routeId : REPLAY_ROUTE_IDS) {
            try {
                if (camel.getRouteController().getRouteStatus(routeId).isStopped()) {
                    camel.getRouteController().startRoute(routeId);
                    LOG.debug("Started route: {}", routeId);
                }
            } catch (Exception e) {
                LOG.error("Failed to start route {}: {}", routeId, e.getMessage());
            }
        }
    }

    private void stopReplayRoutes() {
        LOG.info("Stopping replay routes");
        for (String routeId : REPLAY_ROUTE_IDS) {
            try {
                if (camel.getRouteController().getRouteStatus(routeId).isStarted()) {
                    camel.getRouteController().stopRoute(routeId);
                    LOG.debug("Stopped route: {}", routeId);
                }
            } catch (Exception e) {
                LOG.debug("Route {} already stopped: {}", routeId, e.getMessage());
            }
        }
    }

    public void shutdown() {
        LOG.info("Initiating system shutdown");
        try {
            // Stop health checks first
            if (healthCheckScheduler != null) {
                healthCheckScheduler.shutdownNow();
            }
            
            // Stop Camel context
            if (camel != null) {
                camel.stop();
                LOG.info("Camel context stopped");
            }
            
            // Close HL7 handler
            if (handler != null) {
                handler.close();
                LOG.info("HL7 handler closed");
            }
            
            // Stop health server
            if (healthServer != null) {
                healthServer.stop();
                LOG.info("Health server stopped");
            }
        } catch (Exception e) {
            LOG.error("Shutdown error", e);
        } finally {
            System.exit(1);
        }
    }

    public static boolean isKafkaOperational() {
        return !kafkaDown;
    }
}