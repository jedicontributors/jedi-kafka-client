package jedi.kafka.model;

import lombok.experimental.UtilityClass;

@UtilityClass
public class TestConstants {
  
  public static final String JEDI_KAFKA = "jedi.kafka.*";
  public static final String EMPTY_STRING = "";
  public static final String ERROR_CODE_1 = "-1";
  public static final String PRODUCER = "producer";
  public static final String TEST_CONSUMER = "testConsumer";
  public static final String TEST_TOPIC = "test-topic";
  public static final String OBJECT_TOPIC = "test-object-topic";
  public static final String RETRY_TOPIC = "test-retry";
  public static final String FAILING_TOPIC = "test-failing";
  public static final String TEST = "test";
  public static final String RETRY = "retry";
  public static final String TEST_KAFKA_CONFIG_FILENAME = "test-kafka-config.json";
  public static final long RETRY_10L = 10L;

}
