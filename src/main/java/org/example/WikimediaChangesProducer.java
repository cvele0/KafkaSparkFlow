package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
  private final static Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());
  private final static String bootstrapServers = "localhost:9092";

  public static void main(String[] args) throws InterruptedException {
    log.info("Kafka simple producer");

    // Properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32 * 1024));
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

    KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
    String topic = "wikimedia.recentchange";

    EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);

    String url = "https://stream.wikimedia.org/v2/stream/recentchange";
    EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
    EventSource eventSource = builder.build();

    // Start the producer in another thread
    eventSource.start();

    TimeUnit.MINUTES.sleep(10);


    //Define message - record
//    ProducerRecord<String, String> producerRecord = new ProducerRecord<>("topic1", "Veliki podaci");
//    ProducerRecord<String, String> producerRecord2 = new ProducerRecord<>("topic1", "Veliki podaci 2");

    //Send message
//    producer.send(producerRecord);
//    producer.send(producerRecord2);

//    try (AdminClient adminClient = AdminClient.create(properties)) {
//      NewTopic newTopic = new NewTopic("topic1", 3, (short) 1);
//      adminClient.createTopics(Collections.singletonList(newTopic));
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

//    for (int i = 0; i < 10; i++) {
//      String topic = "topic1";
//      String key = "id_" + i;
//      String value = "Message with value: " + i;
//      ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
//      producer.send(record, new Callback() {
//        @Override
//        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
//          if (e == null) {
//            log.info("Topic: " + recordMetadata.topic() + "\nPartition: " + recordMetadata.partition() + " \nOffset: " + recordMetadata.offset());
//          } else {
//            log.error("Error while producing.", e);
//          }
//        }
//      });
//    }

    // Sync
//    producer.flush();
//
//    producer.close();
  }
}
