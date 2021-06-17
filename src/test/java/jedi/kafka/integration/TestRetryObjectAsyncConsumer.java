package jedi.kafka.integration;

import static jedi.kafka.model.TestConstants.RETRY_TOPIC;

import java.util.stream.IntStream;

import jedi.kafka.model.FailingConsumer;
import jedi.kafka.service.JediKafkaClient;

public class TestRetryObjectAsyncConsumer {

  private static final String TEST_KAFKA_CONFIG_FILE_NAME = "test-retry-object.json";
  private static final int WAIT_10000 = 10000;
  
  public static void main(String[] args) throws InterruptedException {
    JediKafkaClient jediKafkaClient  = JediKafkaClient.getInstance(TEST_KAFKA_CONFIG_FILE_NAME);
    FailingConsumer consumer = new FailingConsumer();
    jediKafkaClient.registerConsumer(RETRY_TOPIC, consumer);
    Thread.currentThread().join(WAIT_10000);
    IntStream.range(0, 10).forEach(item->{
      jediKafkaClient.sendAsync(RETRY_TOPIC, String.valueOf(item));
    });
//    Thread.currentThread().join(WAIT_30000);
//    System.exit(0);
  }
}
