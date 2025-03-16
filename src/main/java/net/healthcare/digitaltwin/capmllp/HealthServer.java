package net.healthcare.digitaltwin.capmllp;

import com.sun.net.httpserver.HttpsServer;
import com.sun.net.httpserver.HttpsConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.SecureRandom;

public class HealthServer {
    private static final Logger LOG = LoggerFactory.getLogger(HealthServer.class);
    private final HttpsServer server;
    private static final String CERT_PATH = "/etc/mllp/secrets/";
    private static final String SERVICE_NAME = System.getenv("KUBERNETES_SERVICE_NAME");
    private static final String NAMESPACE = System.getenv("KUBERNETES_NAMESPACE");

    public HealthServer(int port) throws IOException {
        server = HttpsServer.create(new InetSocketAddress(port), 0);
        
        // SSL Configuration
        try {
            // Keystore path
            String keystorePath = CERT_PATH + SERVICE_NAME + "." + NAMESPACE + ".jks";
            String keystorePassword = System.getenv("KEYSTORE_PASSWORD");

            KeyStore ks = KeyStore.getInstance("PKCS12");
            try (FileInputStream fis = new FileInputStream(keystorePath)) {
                ks.load(fis, keystorePassword.toCharArray());
            }

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keystorePassword.toCharArray());

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), null, new SecureRandom());
            
            server.setHttpsConfigurator(new HttpsConfigurator(sslContext));
        } catch (Exception e) {
            throw new IOException("Failed to configure HTTPS for health server", e);
        }

        // Health endpoint
        server.createContext("/health", exchange -> {
            String response = "OK";
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        });
        server.setExecutor(null); // Default executor
    }

    public void start() {
        LOG.info("Starting HTTPS health server on port {}", server.getAddress().getPort());
        server.start();
    }

    public void stop() {
        LOG.info("Stopping HTTPS health server");
        server.stop(0);
    }
}