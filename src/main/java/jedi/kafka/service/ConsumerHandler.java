package jedi.kafka.service;

import jedi.kafka.model.Response;

public interface ConsumerHandler<T> {
  
  /**
   * Returns jedi.kafka.model.Response
   * @param T element to be consumed
   * @return jedi.kafka.model.Response responseCode 0 for success
   * */
  Response onMessage(T message);
  
  
  /**
   * Default implementation false
   * Returns true if consumer client wants to consume messages as List<T>
   * @return <tt>true</tt> if messages are produced single however consumer client wants to consume as bulk.
   * Can be set to true if consumer client has limited amount of resources like connnection pools.
   * */
  default boolean isBulkConsumer() {
    return false;
  }
  
  /**
   * default implementation is true
   * @return if consumed messaged to be committed synchronously to kafka
   * */
  default boolean isCommitSync() {
    return true;
  }
  
  /**
   * A callback mechanism after all retry attempts exhausted for a failed record.
   * Can be implemented for logging, persisting or any action to take after a failed message.
   * */
  default void onMaxRetryCountReached(T message) {
  }
}
