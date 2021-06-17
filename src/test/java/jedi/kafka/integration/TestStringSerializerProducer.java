package jedi.kafka.integration;

import org.apache.kafka.common.serialization.ByteArraySerializer;

import jedi.kafka.model.ExternalProducer;
import jedi.kafka.model.TestByteArrayConsumer;
import jedi.kafka.service.JediKafkaClient;

public class TestStringSerializerProducer {

  private static final String TEST_TOPIC = "test-byteArr-consumer";

  private static final String TEST_KAFKA_CONFIG_FILE_NAME = "test-byteArr-deserializer-consumer.json";
  
  private static final int WAIT_10000 = 10000;
  
  public static void main(String[] args) throws InterruptedException {
    JediKafkaClient jediKafkaClient  = JediKafkaClient.getInstance(TEST_KAFKA_CONFIG_FILE_NAME);
    jediKafkaClient.registerConsumer(TEST_TOPIC, new TestByteArrayConsumer());
    Thread.currentThread().join(10000);
    ExternalProducer producer = new ExternalProducer(TEST_TOPIC,"0.0.0.0:9092",ByteArraySerializer.class.getName());
    producer.start();
    Thread.currentThread().join(WAIT_10000);
    System.exit(0);
  }
  
  
}
