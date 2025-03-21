package net.healthcare.digitaltwin.capmllp;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageStore {
    private static final Logger LOG = LoggerFactory.getLogger(MessageStore.class);
    private final String basePath;
    private final Map<String, String> topicToFolder;

    public MessageStore(String basePath) {
        this.basePath = basePath.endsWith("/") ? basePath : basePath + "/";
        this.topicToFolder = new HashMap<>();
        topicToFolder.put("AIP-34915", "ADT");
        topicToFolder.put("AIP-34728", "ORU");
        topicToFolder.put("ERROR-QUEUE", "ERROR");
    }

    public void save(String topic, String message, String ack) {
        String folderName = topicToFolder.getOrDefault(topic, "ERROR");
        String msgFolder = basePath + folderName + "/";
        String ackFolder = basePath + folderName + "-ACKS/";

        saveToFolder(msgFolder, message, "msg_" + System.currentTimeMillis() + ".hl7");
        if (ack != null) {
            saveToFolder(ackFolder, ack, "ack_" + System.currentTimeMillis() + ".hl7");
        }
    }

    private void saveToFolder(String folder, String content, String fileName) {
        Path dirPath = Paths.get(folder);
        try {
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }
            Path filePath = dirPath.resolve(fileName);
            try (FileWriter writer = new FileWriter(filePath.toFile(), true)) {
                writer.write(content);
                writer.write("\n");
                LOG.info("Saved to local storage: {}", filePath);
            }
        } catch (IOException e) {
            LOG.error("CRITICAL: Failed to store to {} - DATA LOSS", folder, e);
        }
    }
}