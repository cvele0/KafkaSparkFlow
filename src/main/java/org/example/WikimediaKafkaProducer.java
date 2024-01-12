package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Properties;

public class WikimediaKafkaProducer {
  private static final String KAFKA_TOPIC = "wikimedia-topic";
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String WIKIMEDIA_API_URL = "https://stream.wikimedia.org/v2/stream/recentchange";
  private static Long cnt = 0L;

  private final static Logger log = LoggerFactory.getLogger(WikimediaKafkaProducer.class.getSimpleName());

  public static void main(String[] args) throws IOException {
    // Set up Kafka properties
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create Kafka producer
    Producer<String, String> producer = new KafkaProducer<>(properties);

    // Fetch data from Wikimedia API and send it to Kafka
    fetchWikimediaDataAndSendToKafka(producer);

    // Close the producer
    producer.close();
  }

  private static void fetchWikimediaDataAndSendToKafka(Producer<String, String> producer) throws IOException {
    URL url = new URL(WIKIMEDIA_API_URL);
    HttpURLConnection connection = (HttpURLConnection) url.openConnection();

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        // Send data to Kafka topic
//        cnt++;
//        String key = "line_" + cnt;
        producer.send(new ProducerRecord<>(KAFKA_TOPIC, line));
        System.out.println("Sent to Kafka: " + line);
      }
    } finally {
      connection.disconnect();
    }
  }
}