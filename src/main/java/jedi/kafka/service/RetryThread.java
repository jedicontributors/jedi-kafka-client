package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.EXECUTION_TIME_BUFFER;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

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
      while (!isShutDownInProgress()) {
        if(!retryDuration.equals(getDuration())) {
          log.debug("Polling duration {}",retryDuration);
        }
        ConsumerRecords records = consumer.poll(retryDuration);
        lastPolltime.set(System.currentTimeMillis());
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
    } catch (Exception e) {
      log.warn("Ignoring Exception for shutdown. RetyThread {}", e.getMessage());
    } finally {
      log.info("Closing consumer executor services now..");
      GracefulShutdownStep.shutdownAndAwaitTermination(getExecutorService());
      log.info("Shutdown of executor service finished for topic {} partition(s) {}",topic,consumer.assignment());
      log.info("Closing consumer topic {} partition(s) {}", topic,consumer.assignment());
      consumer.close();
      log.info("Closed consumer for retry topic {} ", topic);
    }
  }
  
  private Duration executeRecord(Iterator<ConsumerRecord<?,?>> iterator,
      KafkaMessage<?> lastProcessedMessage, ConsumerRecord<?,?> record,
      Duration retryDuration, List<Future<?>> futures) {
    log.debug("Retrying message {} for partition {}",lastProcessedMessage.getUniqueId(),record.partition());
    Duration newDuration  = retryDuration;
    Future<?> future = handleRecord(record);
    futures.add(future);
    if(!iterator.hasNext()) {
      waitClientResponse(futures);
      futures.clear();
      log.debug("Resetting retry duration to {} after last record {}",getDuration(),lastProcessedMessage.getUniqueId());
      newDuration = getDuration();
      resumeConsumer();
      commit();
    }
    return newDuration;
  }

}