package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.EXECUTION_TIME_BUFFER;
import static jedi.kafka.model.KafkaConstants.NEXT_RETRY_TIMESTAMP;
import static jedi.kafka.model.KafkaConstants.RETRY_COUNTER;
import static jedi.kafka.model.KafkaConstants.UNQUE_ID;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;

import jedi.kafka.model.KafkaMessage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RetryThread extends ConsumerThread  {

  @SuppressWarnings({"unchecked", "rawtypes"})
  @Override
  public void run() {
    String topic = getConsumerConfig().getTopic();
    Consumer<?,?> consumer = getKafkaConsumer();
    try {
      consumer.subscribe(Collections.singleton(topic));
      log.info("Topic {} subscription completed", topic);
      Iterator<ConsumerRecord<?,?>> iterator = null;
      KafkaMessage<?> lastProcessedMessage = null;
      ConsumerRecord<?,?> record = null;
      Duration retryDuration = getDuration();
      List<Future<?>> futures = null;
      ConsumerRecords records = null;
      while (!isShutDownInProgress()) {
        if(!retryDuration.equals(getDuration())) {
          log.debug("Polling duration {}",retryDuration);
        }
        records = poll(retryDuration);
        if(!consumerPaused.get()){
          iterator = records.iterator();
          futures = new ArrayList<>();
        }
        while(iterator.hasNext()||consumerPaused.get()) {
          if(Objects.isNull(lastProcessedMessage)) {
            record = iterator.next();
            lastProcessedMessage = getKafkaMessage(record);  
          }
          long remainingTime = lastProcessedMessage.getNextRetryTimestamp().longValue() - Instant.now().toEpochMilli();
          if (remainingTime <= EXECUTION_TIME_BUFFER) {
            retryDuration = executeRecord(iterator, lastProcessedMessage, record, retryDuration, futures);
            lastProcessedMessage  = null;
          }else {
            retryDuration = Duration.ofMillis(remainingTime);
            pauseConsumer(record,lastProcessedMessage.getUniqueId());
            break;
          } 
        }
      }
    }catch (WakeupException e) {
      log.warn("Consumer waked up for topic {} partitions {}",getConsumerConfig().getTopic(),consumer.assignment());
    } catch (Exception e) {
      log.warn("Ignoring Exception for shutdown. RetyThread {}", e.getMessage());
    } finally {
      String partitions = consumer.assignment().toString();
      log.info("Closing consumer topic {} partition(s) {}", topic,partitions);
      consumer.close();
      log.info("Closed consumer topic {} partition(s) {}", topic,partitions);
      log.info("Closing retry consumer executor services for topic {} partitions(s) {}",topic,partitions);
      GracefulShutdownStep.shutdownAndAwaitTermination(getExecutorService());
      log.info("Closed consumer executor services for topic {} partition(s)  {}", topic,partitions);
      countDownLatch.countDown();
      log.info("Shutdown complete for topic {} partition(s)  {}",topic,partitions);
    }
  }
  
  private Duration executeRecord(Iterator<ConsumerRecord<?,?>> iterator,
      KafkaMessage<?> lastProcessedMessage, ConsumerRecord<?,?> record,
      Duration retryDuration, List<Future<?>> futures) {
    log.debug("Retrying message {} for partition {}",lastProcessedMessage.getUniqueId(),record.partition());
    Duration newDuration  = retryDuration;
    Future<?> future = handleRecord(record);
    futures.add(future);
    if(isShutDownInProgress()) {
      String topic = getConsumerConfig().getTopic();
      log.info("Trying to reproduce any records remaining for topic {}",topic);
      int remainingItemsInBatch = 0;
      ConsumerRecord<?,?> remainingRecord = null;
      while(Objects.nonNull(iterator) && iterator.hasNext()) {
        remainingRecord = iterator.next();
        KafkaMessage<?> kafkaMessage = getKafkaMessage(remainingRecord);
        ProducerRecord<?,?> producerRecord = getKafkaService().createProducerRecord(topic,(Serializable)kafkaMessage.getMessage());
        producerRecord.headers().add(UNQUE_ID,SerializationUtils.serialize(kafkaMessage.getUniqueId()));
        producerRecord.headers().add(RETRY_COUNTER, SerializationUtils.serialize(kafkaMessage.getRetryCounter()));
        producerRecord.headers().add(NEXT_RETRY_TIMESTAMP, SerializationUtils.serialize(kafkaMessage.getNextRetryTimestamp()));
        getKafkaService().sendAsync(getConsumerConfig().getTopic(),producerRecord);
        remainingItemsInBatch++;
      }
      log.info("Reproduced {} items in retry batch to topic {}",remainingItemsInBatch,topic);
    }
    if(!iterator.hasNext()) {
      waitClientResponse(futures);
      log.debug("Resetting retry duration to {} after last record {}",getDuration(),lastProcessedMessage.getUniqueId());
      newDuration = getDuration();
      resumeConsumer();
      commit();
    }
    return newDuration;
  }
}