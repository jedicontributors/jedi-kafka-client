package jedi.kafka.model;

import jedi.kafka.service.KafkaService;

public class MockedKafkaService extends KafkaService {

  public MockedKafkaService() {
    super(TestConstants.TEST_KAFKA_CONFIG_FILENAME);
  }
  
  public MockedKafkaService(String filename) {
    super(filename);
  }
  
}