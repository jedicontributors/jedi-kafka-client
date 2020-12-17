package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.BLANK_DELIMITER;
import static jedi.kafka.model.KafkaConstants.EXECUTION_TIME_BUFFER;
import static jedi.kafka.model.KafkaConstants.FAST_POLL_TIME;
import static jedi.kafka.model.KafkaConstants.NEXT_RETRY_TIMESTAMP;
import static jedi.kafka.model.KafkaConstants.RETRY_COUNTER;
import static jedi.kafka.model.KafkaConstants.RETRY_INITIAL_VALUE;
import static jedi.kafka.model.KafkaConstants.UNQUE_ID;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

import jedi.kafka.model.KafkaMessage;
import jedi.kafka.model.KafkaServiceConfig.KafkaConsumerConfig;
import jedi.kafka.model.Response;
import jedi.kafka.model.RetryPolicy;
import jedi.kafka.util.ThreadPoolUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
@Setter
public class ConsumerThread extends Thread {


  protected final AtomicBoolean consumerPaused = new AtomicBoolean(false);
  protected final AtomicLong lastPolltime = new AtomicLong();

  protected Collection<TopicPartition> assignedPartitions = new HashSet<>();
  
  private long maxProcessTime;
  private KafkaService kafkaService;
  private KafkaConsumerConfig<?,?> consumerConfig;
  @SuppressWarnings("rawtypes")
  private ConsumerHandler handler;
  private Duration duration;
  private boolean isRetryable;
  private ExecutorService  executorService;
  private Consumer<?,?> kafkaConsumer;
  private boolean isShutDownInProgress;
  private RebalanceListener rebalanceListener;
  
  @Override
  public void run() {
    String topic = consumerConfig.getTopic();
    ConsumerRecords<?,?> records = null;
    try {
      kafkaConsumer.subscribe(Collections.singleton(topic));
      log.info("Topic {} subscription completed",topic);
      if(handler.isBulkConsumer()) {
        while (!isShutDownInProgress) {
          records = kafkaConsumer.poll(duration);
          lastPolltime.set(System.currentTimeMillis());
          if(!records.isEmpty()) {
            consumeBulk(records);
            resumeConsumer();
            commit();
          }
        }
      }else {
        while (!isShutDownInProgress) {
          records = kafkaConsumer.poll(duration);
          lastPolltime.set(System.currentTimeMillis());
          if(!records.isEmpty()) {
            consume(records);
            resumeConsumer();
            commit();
          }
        }
      }
    }catch (WakeupException e) {
      log.warn("Ignoring WakeupException");
    } catch(Exception ex){
        log.error("Exception in ConsumerThread ",ex);  
    } finally {
      log.info("Closing consumer executor services now..");
      GracefulShutdownStep.shutdownAndAwaitTermination(executorService);
      log.info("Shutdown of executor service finished for topic {} partition(s) {}",topic,kafkaConsumer.assignment());
      log.info("Closing consumer topic {} partition(s) {}", topic,kafkaConsumer.assignment());
      kafkaConsumer.close();
      log.info("Closed consumer with a partition for topic {} ", topic);
    }
  }

  @SuppressWarnings("unchecked")
  protected Future<?> handleRecord(ConsumerRecord<?,?> record) {
    return executorService.submit(() -> {
      KafkaMessage<?> kafkaMessage = null;
      try {
        Response handlerResponse = null;
        kafkaMessage = getKafkaMessage(record);
        Object message = kafkaMessage.getMessage();
        if(handler.isBulkConsumer()) {
          handlerResponse = handler.onMessage(Arrays.asList(message));
        }else {
          handlerResponse = handler.onMessage(message);
        }
        if(canRetry(handlerResponse)) {
          retry(kafkaMessage, handlerResponse);
        }
      } catch (Exception e) {
        if(Objects.nonNull(record.value())) {
          log.error("{} - Consumer Exception ",kafkaMessage.getUniqueId(),e);
        }else {
          log.error("Consumer Exception",e);
        }
      }
    });
  }
  
  protected void waitClientResponse(List<Future<?>> futureList) {
    preventRebalance();
    for(Future<?> future:futureList) {
      waitClientResponse(future);
    }
  }

  protected void resumeConsumer() {
    if(consumerPaused.get()) {
      String partitions = (String)kafkaConsumer.assignment()
          .stream()
          .map(tp->String.valueOf(tp.partition()))
          .collect(Collectors.joining(BLANK_DELIMITER));
      log.info("Resuming topic {} for partition {} now..",consumerConfig.getTopic(),partitions);
      kafkaConsumer.resume(kafkaConsumer.assignment());
      consumerPaused.set(false);
    }
  }

  @SuppressWarnings("unchecked")
  protected boolean retry(KafkaMessage<?> kafkaMessage,Response response) throws InterruptedException, ExecutionException {
    if(Objects.isNull(response)) {
      return false;
    }
    Integer retryCounter = Objects.isNull(kafkaMessage.getRetryCounter())?RETRY_INITIAL_VALUE:kafkaMessage.getRetryCounter(); 
    UUID uniqueId = Objects.isNull(kafkaMessage.getUniqueId())?UUID.randomUUID():kafkaMessage.getUniqueId();
    if(retryCounter.intValue() < consumerConfig.getMaxRetryCount().intValue()) {
      RetryPolicy retryPolicy = consumerConfig.getRetryPolicys().get(String.valueOf(response.getResponseCode()));
      if(Objects.nonNull(retryPolicy)) {
        ProducerRecord<?,?> producerRecord = kafkaService.createProducerRecord(retryPolicy.getRetryTopic(),(Serializable)kafkaMessage.getMessage());
        producerRecord.headers().add(UNQUE_ID,SerializationUtils.serialize(uniqueId));
        producerRecord.headers().add(RETRY_COUNTER, SerializationUtils.serialize(retryCounter+1));
        producerRecord.headers().add(NEXT_RETRY_TIMESTAMP, SerializationUtils.serialize(Instant.now().toEpochMilli() + retryPolicy.getSeconds()*1000));
        log.debug("Sending message {} to topic {}",uniqueId,retryPolicy.getRetryTopic());
        kafkaService.sendAsync(retryPolicy.getRetryTopic(),producerRecord);
        return true;
      }
    }else {
      log.info("{} MaxRetryCount {} reached.",uniqueId,consumerConfig.getMaxRetryCount());
      try {
        handler.onMaxRetryCountReached(kafkaMessage.getMessage());
      } catch (Exception e) {
        log.error("Error on onMaxRetryCountReached implementation",e);
      }
    }
    return false;
  }
  
  protected void commit() {
    try {
      if(handler.isCommitSync()) {
        kafkaConsumer.commitSync();
      }else {
        kafkaConsumer.commitAsync();
      }
      log.debug("Committed messages for {}",this.consumerConfig.getTopic());
    } catch (Exception e) {
      log.error("Error on committing messages",e);
    }
  }

  protected static Object deserialize(byte[] bytes) {
    if (Objects.isNull(bytes)) {
      return bytes;
    }
    try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return ois.readObject();
    } catch (IOException ex) {
      throw new IllegalArgumentException("Failed to deserialize object", ex);
    } catch (ClassNotFoundException ex) {
      throw new IllegalStateException("Failed to deserialize object type", ex);
    }
  }
  
  protected void shutdown() {
    log.info("Shutdown request recieved for consumer -> {}",this.getName());
    this.isShutDownInProgress=true;
  }
  
  protected void close() {
    log.info("Closing consumer topic -> {}",consumerConfig.getTopic());
    kafkaConsumer.close();
  }

  protected void initialize(KafkaService kafkaService,ConsumerThread consumerThread,KafkaConsumerConfig<?,?> consumerConfig,ConsumerHandler<?> consumerHandler,
      Duration duration,String threadName,long maxProcessTime) {
    consumerThread.setKafkaService(kafkaService);
    consumerThread.setConsumerConfig(consumerConfig);
    consumerThread.setHandler(consumerHandler);
    consumerThread.setDuration(duration);
    consumerThread.setName(threadName);
    consumerThread.setRetryable(Objects.nonNull(consumerConfig.getRetryPolicys()) && !consumerConfig.getRetryPolicys().isEmpty());
    consumerThread.setExecutorService(ThreadPoolUtil.newCachedLimitedThreadPool(threadName,consumerConfig.getMaximumThreadCount()));
    consumerThread.setMaxProcessTime(maxProcessTime);
    consumerThread.setKafkaConsumer(new KafkaConsumer<>(consumerConfig.getProperties()));
  }
  
  private Object getMessage(ConsumerRecord<?,?> record) {
    return consumerConfig.isValueSerDesByteArray()?record.value():SerializationUtils.deserialize((byte[])record.value());
  }

  protected KafkaMessage<?> getKafkaMessage(ConsumerRecord<?,?> record) {
    Object message = getMessage(record);
    KafkaMessage<?> kafkaMessage = new KafkaMessage<>(message);
    addHeaderInfoToKafkaMessage(record, kafkaMessage);
    return kafkaMessage;
  }
  
  protected void pauseConsumer(ConsumerRecord<?,?> record,UUID uuid) {
    if(!consumerPaused.get()) {
      kafkaConsumer.pause(kafkaConsumer.assignment());
      if(Objects.nonNull(record)&&Objects.nonNull(uuid)) {
        log.debug("Pausing {} consumer for partition {} for record {} now..",consumerConfig.getTopic(),record.partition(),uuid);
      }else {
        log.debug("Pausing {} consumer..",consumerConfig.getTopic());
      }
      consumerPaused.set(true);
    }
  }
  
  protected void preventRebalance() {
    if(System.currentTimeMillis()-lastPolltime.get() + EXECUTION_TIME_BUFFER < maxProcessTime) {
      return;
    }
    log.warn("Messsage processing takes too long.");
    pauseConsumer(null,null);
    kafkaConsumer.poll(FAST_POLL_TIME);
    lastPolltime.set(System.currentTimeMillis());
  }
  
  private void consume(ConsumerRecords<?,?> records) {
    List<Future<?>> futureList = new ArrayList<>();
    for(ConsumerRecord<?,?> record :records) {
      futureList.add(handleRecord(record));
    }
    waitClientResponse(futureList);
  }
  
  private void addHeaderInfoToKafkaMessage(ConsumerRecord<?, ?> record,KafkaMessage<?> kafkaMessage) {
    Optional<?> uniqueId = getHeaderInfo(record, UNQUE_ID);
    if(uniqueId.isPresent()) {
      kafkaMessage.setUniqueId((UUID)uniqueId.get());
    }
    Optional<?> retryCounter = getHeaderInfo(record, RETRY_COUNTER);
    if(retryCounter.isPresent()) {
      kafkaMessage.setRetryCounter((Integer)retryCounter.get());
    }
    Optional<?> nextRetryTimeStamp = getHeaderInfo(record, NEXT_RETRY_TIMESTAMP);
    if(nextRetryTimeStamp.isPresent()) {
      kafkaMessage.setNextRetryTimestamp((Long)nextRetryTimeStamp.get());
    }
  }

  private Optional<Object> getHeaderInfo(ConsumerRecord<?, ?> record,String key) {
    Iterable<Header> iterator = record.headers().headers(key);
    if(iterator.iterator().hasNext()) {
      return Optional.of(SerializationUtils.deserialize(record.headers().headers(key).iterator().next().value()));
    }
    return Optional.empty();
  }
  
  private void consumeBulk(ConsumerRecords<?,?> records) {
    List<KafkaMessage<?>> kafkaMessages = new ArrayList<>();
    List<Object> messages = new ArrayList<>();
    try {
      for(ConsumerRecord<?,?> record : records) {
        KafkaMessage<?> kafkaMessage = getKafkaMessage(record);
        try {
          messages.add(kafkaMessage.getMessage());
        } catch (Exception e) {
          e.printStackTrace();
        }
        if(isRetryable) {
          kafkaMessages.add(kafkaMessage);
        }
      }
      @SuppressWarnings("unchecked")
      Future<?> handlerFuture = executorService.submit(() -> {
        try {
          Response handlerResponse = handler.onMessage(messages);
          if(canRetry(handlerResponse)) {
            kafkaMessages.forEach(kafkaMessage->{
              try {
                retry(kafkaMessage, handlerResponse);
              } catch (Exception e) {
                log.error("{} - Bulk Consumer Retry Exception",messages,e);
              }
            });
          }
        } catch (Exception e) {
            log.error("{} - Bulk Consumer Exception",e);
        }
      });
      waitClientResponse(handlerFuture);
    }catch (Exception e) {
      log.error("{} - Bulk Consumer Exception",messages,e);
    }
  }
  
  private void waitClientResponse(Future<?> future) {
	boolean isProcessing = true;
    while(isProcessing) {
      try {
        future.get(maxProcessTime,TimeUnit.MILLISECONDS);
        isProcessing = false;
      }catch(TimeoutException timeoutException ) {
        preventRebalance();
      } catch (Exception e) {
        log.error("Failed to process batch of records.",e);
        isProcessing = false;
      }
    }
  }

  private boolean canRetry(Response handlerResponse) {
    return isRetryable && Objects.nonNull(handlerResponse) && !handlerResponse.isSuccess();
  }
}
