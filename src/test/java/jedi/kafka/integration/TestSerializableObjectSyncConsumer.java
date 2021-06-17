package jedi.kafka.integration;

import static jedi.kafka.model.TestConstants.OBJECT_TOPIC;

import java.util.stream.IntStream;

import jedi.kafka.model.AnySerializableObject;
import jedi.kafka.model.AnySerializableObjectConsumer;
import jedi.kafka.service.JediKafkaClient;

public class TestSerializableObjectSyncConsumer {

  private static final int WAIT_10000 = 10000;
  
  private static final String TEST_KAFKA_CONFIG_FILE_NAME = "test-kafka-config-object.json";

  public static void main(String[] args) throws InterruptedException {
    JediKafkaClient jediKafkaClient  = JediKafkaClient.getInstance(TEST_KAFKA_CONFIG_FILE_NAME);
    AnySerializableObjectConsumer messageConsumer = new AnySerializableObjectConsumer();
    jediKafkaClient.registerConsumer(OBJECT_TOPIC, messageConsumer);
    Thread.currentThread().join(WAIT_10000);
    IntStream.range(0, 10).forEach(item->{
      jediKafkaClient.sendSync(OBJECT_TOPIC, new AnySerializableObject(String.valueOf(item)));
    });
  }
}
