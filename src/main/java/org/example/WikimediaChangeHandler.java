package org.example;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class WikimediaChangeHandler implements EventHandler {
  private final static Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

  private final KafkaProducer<String, String> kafkaProducer;
  private final String topic;

  public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic) {
    this.kafkaProducer = kafkaProducer;
    this.topic = topic;
  }

  @Override
  public void onOpen() throws Exception { }

  @Override
  public void onClosed() throws Exception {
    kafkaProducer.close();
  }

  @Override
  public void onMessage(String s, MessageEvent messageEvent) throws Exception {
    log.info(messageEvent.getData());
    // Async
    kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
  }

  @Override
  public void onComment(String s) throws Exception {

  }

  @Override
  public void onError(Throwable throwable) {
    log.info("Error in stream reading ", throwable);
  }

  public static void main(String[] args) {
    log.info("Kafka simple consumer");

    String groupId = "my-application2";
    String topic = "topic1";

    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // cashing, re-balancing
    properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

    consumer.subscribe(List.of(topic));

    while (true) {
      log.info("polling");
      ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
      for (ConsumerRecord<String, String> record : records) {
        log.info("Key: " + record.key() + " , Value: " + record.value() +
                "\n Partition: " + record.partition() + "\n Offset: " + record.offset());
      }
    }
  }
}
