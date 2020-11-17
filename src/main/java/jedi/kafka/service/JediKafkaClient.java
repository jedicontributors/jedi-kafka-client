package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.KAFKA_PROPERTIES_FILE;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;

public class JediKafkaClient {

  private static Map<String, JediKafkaClient> jediKafkaClientMap = new HashMap<>();

  private KafkaService kafkaService;
  
  /**
   * Returns instance of JediKafkaClient from kafka-config.json file
   * <p>
   * 
   * <p>
   * 
   * @return KafkaService
   */
  public static JediKafkaClient getInstance() {
    return getOrCreateInstance(KAFKA_PROPERTIES_FILE);
  }

  /**
   * Returns instance of JediKafkaClient for filename provided
   * 
   * @param fileName for kafka configuration
   * @return KafkaService
   */
  public static JediKafkaClient getInstance(String kafkaConfigfileName) {
    return getOrCreateInstance(kafkaConfigfileName);
  }
  
  /**
   * A new JediKafkaClient is constructed based on kafka-config.json file from classpath resource
   * <p>
   * o
   * <p>
   */
  private JediKafkaClient(KafkaService kafkaService) {
    this.kafkaService = kafkaService;
  }
  
  /**
   * Consumer starts for input topic and ConsumerHandler implementation
   * <p>
   * Caller must provide which topic to consume and ConsumerHandler implementation
   * <p>
   * Note: after graceful shutdown, consumer and its thread-pool also terminates
   *
   * @param topic To create producer/consumer for topic
   * @param ConsumerHandler implementation for processing messages
   */
  public void registerConsumer(String topic, ConsumerHandler<?> consumerHandler) {
    kafkaService.registerConsumer(topic, consumerHandler);
  }
  
  /**
   * Sending message synchronously to kafka
   * 
   * @param topic Topic to send message
   * @param message Any Object that implements Serializable interface
   * @return unique id reference
   * @throws ExecutionException
   * @throws InterruptedException
   */
  public UUID sendSync(String topic, Serializable message) {
    return kafkaService.sendSync(topic, message);
  }
  
  /**
   * Sending message asynchronously to kafka
   * 
   * @param topic Topic to send message
   * @param message Any Object that implements Serializable interface
   * @return unique id reference
   */
  public UUID sendAsync(String topic, Serializable message) {
    return kafkaService.sendAsync(topic, message);
  }
  
  /**
   * Sending message asynchronously to kafka with callback
   * 
   * @param topic Topic to send message
   * @param message Any Object that implements Serializable interface
   * @param callback Any implementation class of a Callback interface
   * @return unique id reference
   */
  public UUID sendAsyncWithCallback(String topic, Serializable message, Callback callback) {
    return kafkaService.sendAsyncWithCallback(topic, message, callback);
  }
  
  /**
   * Checks if errorCode is retryable for topic and the KafkaService
   * <p>
   * 
   * <p>
   * 
   * @param topic kafkaService topic
   * @param errorCode
   * @return true if errorCode was defined in RetryPolicy for the topic
   */
  public boolean isRetryableErrorCode(String topic, String errorCode) {
    return kafkaService.isRetryableErrorCode(topic, errorCode);
  }
  
  /**
   * Checks availability of all brokers in configuration file
   * @return true if all brokers are available
   */
  public boolean isBrokersAvailable() {
    return kafkaService.isBrokersAvailable();
  }
  
  private static JediKafkaClient getOrCreateInstance(String kafkaConfigfileName) {
    if(Objects.isNull(jediKafkaClientMap.get(kafkaConfigfileName))) {
      KafkaService kafkaService = new KafkaService(kafkaConfigfileName);
      JediKafkaClient jediKafkaClient = new JediKafkaClient(kafkaService);
      jediKafkaClientMap.put(kafkaConfigfileName, jediKafkaClient);
    }
    return jediKafkaClientMap.get(kafkaConfigfileName);
  }
  
}
