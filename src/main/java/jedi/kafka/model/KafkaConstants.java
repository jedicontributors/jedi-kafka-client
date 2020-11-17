package jedi.kafka.model;

import java.time.Duration;

import lombok.experimental.UtilityClass;

@UtilityClass
public class KafkaConstants {
  
  public static final String UNQUE_ID = "uniqueId";
  
  public static final String NEXT_RETRY_TIMESTAMP = "nextRetryTimestamp";

  public static final String RETRY_COUNTER = "retryCounter";
  
  public static final Integer RETRY_INITIAL_VALUE = Integer.valueOf(0);
  
  public static final String KAFKA_PROPERTIES_FILE = "/kafka-config.json";
  
  public static final String CONTEXT_SEPERATOR = "/";

  public static final String UTF_8 = "UTF-8";

  public static final int SECOND_TO_MILISECOND_COEFFICIENT = 1000;
  
  public static final int EXECUTION_TIME_BUFFER = 1000;
  
  public static final long BUFFER_TIME_FOR_EXECUTION = 3000; 
  
  public  static final String GROUP = "group";

  public  static final String DASH = "-";
  
  public static final String PRODUCER_POOL = "ProducerPool";

  public static final String KAFKA_SERVICE = "KafkaService";

  public static final String KAFKA_SENDER_THREAD_PREFIX = "kafkaSender";

  public static final String GROUP_ID = "group.id";

  public static final int MAX_RETRY_COUNT = 3;

  public static final Integer DEFAULT_PARTITION_COUNT = Integer.valueOf(3);
  
  public static final int DEFAULT_MAX_RETRY_COUNT = 10;
  
  public static final int MAX_PROCESS_TIME_SECONDS = 30;
  
  public static final String BLANK_DELIMITER = " ";
  
  public static final long DEFAULT_MAX_PROCESS_TIME = 30000;
  
  public static final Duration FAST_POLL_TIME = Duration.ofMillis(10);

  public static final String DEFAULT_MAX_POLL_RECORDS = "500";

  public static final String DEFAULT_SESSION_TIMEOUT_MS = "60000";
  
  public static final String DEFAULT_MAX_POLL_INTERVAL_MS = DEFAULT_SESSION_TIMEOUT_MS;
  
  public static final String DEFAULT_RECONNECT_BACKOFF_MS = "3000";
  
  public static final String DEFAULT_RECONNECT_BACKOFF_MAX_MS = "3000";

  public static final String MAX_POLL_RECORDS = "max.poll.records";
  
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";
  
  public static final String SESSION_TIMEOUT_MS = "session.timeout.ms";

  public static final String ENABLE_AUTO_COMMIT = "enable.auto.commit";
  
  public static final String REQUEST_TIMEOUT_MS = "request.timeout.ms";
  
  public static final String MAX_POLL_INTERVAL_MS = "max.poll.interval.ms";
  
  public static final String RECONNECT_BACKOFF_MS = "reconnect.backoff.ms";
  
  public static final String RECONNECT_BACKOFF_MAX_MS = "reconnect.backoff.max.ms";
  
  public static final String KEY_SERIALIZER = "key.serializer";
  
  public static final String VALUE_SERIALIZER = "value.serializer";
  
  public static final String KEY_DESERIALIZER = "key.deserializer";
  
  public static final String VALUE_DESERIALIZER = "value.deserializer";
  
  public static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  
  public static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
  
  public static final String BYTE_ARRAY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  
  public static final String BYTE_ARRAY_DESERIALIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  
  public static final String SAMPLE_PRODUCER_CONSUMER_CONFIGURATION = 
      "Sample Producer Configuration \n"
          + "  \"producers\": {\n" + 
          "        \"producerAlias\": {\n" + 
          "            \"topic\": \"topic\",\n" + 
          "            \"properties\": {\n" + 
          "                \"bootstrap.servers\": \"CSV of Kafka Brokers's IP addresses \",\n" + 
          "            }\n" + 
          "        }\n" + 
          "    },\n"+
          "Sample Consumer Configuration \n"+
          "\"ConsumerAlias\": {\r\n" + 
          "            \"topic\": \"topic\",\r\n" + 
          "            \"properties\": {\r\n" + 
          "                \"bootstrap.servers\": \"CSV IP addresses of Kafka Brokers\",\r\n" + 
          "                \"group.id\": \"ConsumerAlias-group\",\r\n" + 
          "            }\r\n" + 
          "         }";
  
}
