package jedi.kafka.integration;

import static jedi.kafka.model.TestConstants.OBJECT_TOPIC;

import java.util.stream.IntStream;

import jedi.kafka.model.BulkConsumer;
import jedi.kafka.service.JediKafkaClient;

public class TestStringBulkSyncConsumer {

  private static final int WAIT_10000 = 10000;
  
  private static final String TEST_KAFKA_CONFIG_FILE_NAME = "test-kafka-config-object.json";

  public static void main(String[] args) throws InterruptedException {
    JediKafkaClient jediKafkaClient  = JediKafkaClient.getInstance(TEST_KAFKA_CONFIG_FILE_NAME);
    BulkConsumer bulkConsumer = new BulkConsumer();
    jediKafkaClient.registerConsumer(OBJECT_TOPIC, bulkConsumer);
    Thread.currentThread().join(WAIT_10000);
    IntStream.range(0, 10).forEach(item->{
      jediKafkaClient.sendSync(OBJECT_TOPIC, String.valueOf(item));
    });
    Thread.currentThread().join(WAIT_10000);
    System.exit(0);
  }

}
