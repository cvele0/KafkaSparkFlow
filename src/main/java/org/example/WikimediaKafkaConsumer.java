package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collections;
import java.util.Properties;

public class WikimediaKafkaConsumer {
  private static final Logger log = LoggerFactory.getLogger(WikimediaKafkaConsumer.class);
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  private static final String KAFKA_TOPIC = "wikimedia-topic";

  public static void main(String[] args) {
    // Set up Kafka consumer properties
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "wikimedia-consumer-group");
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.put("value.deserializer.encoding", "UTF-8"); // Specify the character encoding

    // Create Kafka consumer
    Consumer<String, String> consumer = new KafkaConsumer<>(properties);

    // Subscribe to the Kafka topic
    consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

    // Start consuming messages
    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

      // Process each record
      records.forEach(record -> {
        log.info("Received record:");
        log.info("Key: {}", record.key());
        log.info("Value: {}", record.value());

        // Parse and process the JSON data
        processWikimediaEventData(record.value());
      });
    }
  }

  private static void processWikimediaEventData(String jsonData) {
    try {
      // Extract JSON content after "data:" prefix
      int dataIndex = jsonData.indexOf("data:");
      if (dataIndex == -1) {
        log.warn("\"data:\" not found in the message: {}", jsonData);
        return;
      }

      String jsonPart = jsonData.substring(dataIndex + "data:".length()).trim();

      // Use Jackson ObjectMapper to parse JSON data
      ObjectMapper objectMapper = new ObjectMapper();
      JsonNode jsonNode = objectMapper.readTree(jsonPart);

      // Check if there is a "type" key in the JSON structure
      String eventType = jsonNode.has("type") ? jsonNode.get("type").asText() : "";

      // Check if there is a "title" key in the JSON structure
      String title = jsonNode.has("title") ? jsonNode.get("title").asText() : "";

      // Check if there is a "user" key in the JSON structure
      String user = jsonNode.has("user") ? jsonNode.get("user").asText() : "";

      // Check if there is a "timestamp" key in the JSON structure
      String timestamp = jsonNode.has("timestamp") ? jsonNode.get("timestamp").asText() : "";

      // Additional checks for optional fields
      String notifyUrl = jsonNode.has("notify_url") ? jsonNode.get("notify_url").asText() : "";
      String oldLength = jsonNode.has("length") && jsonNode.get("length").has("old") ? jsonNode.get("length").get("old").asText() : "0";
      String newLength = jsonNode.has("length") && jsonNode.get("length").has("new") ? jsonNode.get("length").get("new").asText() : "0";

      // Log the extracted information
      log.info("Event Type: {}", eventType);
      log.info("Title: {}", title);
      log.info("User: {}", user);
      log.info("Timestamp: {}", timestamp);
      log.info("Notify URL: {}", notifyUrl);
      log.info("Old Length: {}", oldLength);
      log.info("New Length: {}", newLength);

      // Save the information to a CSV file
      saveToCsv(eventType, title, user, timestamp, oldLength, newLength);

    } catch (IOException e) {
      log.error("Error processing Wikimedia Event Data: {}", e.getMessage());
    }
  }

  private static void saveToCsv(String eventType, String title, String user, String timestamp, String oldLength, String newLength) {
    // Define the CSV file path
    if (eventType.isEmpty() && title.isEmpty() && user.isEmpty() && timestamp.isEmpty()) return;
    String csvFilePath = "wikimedia_events.csv";

    try (FileWriter writer = new FileWriter(csvFilePath, true)) {
      // Append data to the CSV file
      writer.append(String.join(",", eventType, title, user, timestamp, oldLength, newLength));
      writer.append("\n");

      log.info("Data saved to CSV file: {}", csvFilePath);

    } catch (IOException e) {
      log.error("Error saving data to CSV file: {}", e.getMessage());
      e.printStackTrace(); // Add this line to print the full stack trace
    }
  }
}

