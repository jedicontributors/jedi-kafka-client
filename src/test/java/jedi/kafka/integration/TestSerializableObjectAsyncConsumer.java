package jedi.kafka.integration;

import static jedi.kafka.model.TestConstants.OBJECT_TOPIC;

import java.util.stream.IntStream;

import jedi.kafka.model.AnySerializableObject;
import jedi.kafka.model.AnySerializableObjectConsumer;
import jedi.kafka.service.JediKafkaClient;

public class TestSerializableObjectAsyncConsumer {

  private static final String TEST_KAFKA_CONFIG_FILE_NAME = "test-kafka-config-object.json";
  private static final int WAIT_10000 = 10000;

  public static void main(String[] args) throws InterruptedException {
    JediKafkaClient jediKafkaClient  = JediKafkaClient.getInstance(TEST_KAFKA_CONFIG_FILE_NAME);
    AnySerializableObjectConsumer consumer = new AnySerializableObjectConsumer();
    jediKafkaClient.registerConsumer(OBJECT_TOPIC, consumer);
    Thread.currentThread().join(WAIT_10000);
    IntStream.range(0, 10).forEach(item->{
      jediKafkaClient.sendAsync(OBJECT_TOPIC, new AnySerializableObject(String.valueOf(item)));
    });
//    System.exit(0);
  }
}
