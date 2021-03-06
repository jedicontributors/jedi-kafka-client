package jedi.kafka.integration;

import java.util.stream.IntStream;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import jedi.kafka.model.ExternalConsumer;
import jedi.kafka.service.JediKafkaClient;

public class TestByteArrayDeserializerConsumer {

  private static final String TEST_TOPIC = "test-byteArr-producer";

  private static final String TEST_KAFKA_CONFIG_FILE_NAME = "test-byteArr-serializer-producer.json";
  
  private static final int WAIT_10000 = 10000;
  
  public static void main(String[] args) throws InterruptedException {
    ExternalConsumer consumer = new ExternalConsumer(TEST_TOPIC,"0.0.0.0:9092",ByteArrayDeserializer.class.getName());
    consumer.start();
    Thread.currentThread().join(WAIT_10000);
    JediKafkaClient jediKafkaClient  = JediKafkaClient.getInstance(TEST_KAFKA_CONFIG_FILE_NAME);
    IntStream.range(0, 10).forEach(item->{
      jediKafkaClient.sendSync(TEST_TOPIC, SerializationUtils.serialize(String.valueOf(item)));
    });
    Thread.currentThread().join(WAIT_10000);
    consumer.close();
    System.exit(0);
  }
  
  
}
