package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.BOOTSTRAP_SERVERS;
import static jedi.kafka.model.KafkaConstants.DASH;
import static jedi.kafka.model.KafkaConstants.DEFAULT_MAX_POLL_INTERVAL_MS;
import static jedi.kafka.model.KafkaConstants.DEFAULT_MAX_PROCESS_TIME;
import static jedi.kafka.model.KafkaConstants.DEFAULT_MAX_RETRY_COUNT;
import static jedi.kafka.model.KafkaConstants.DEFAULT_PARTITION_COUNT;
import static jedi.kafka.model.KafkaConstants.MAX_POLL_INTERVAL_MS;
import static jedi.kafka.model.KafkaConstants.PRODUCER_POOL;
import static jedi.kafka.model.KafkaConstants.SAMPLE_PRODUCER_CONSUMER_CONFIGURATION;
import static jedi.kafka.model.KafkaConstants.UNQUE_ID;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import jedi.kafka.model.Errors;
import jedi.kafka.model.KafkaRuntimeException;
import jedi.kafka.model.KafkaServiceConfig;
import jedi.kafka.model.KafkaServiceConfig.KafkaConsumerConfig;
import jedi.kafka.model.KafkaServiceConfig.KafkaProducerConfig;
import jedi.kafka.model.RetryPolicy;
import jedi.kafka.util.ThreadPoolUtil;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@SuppressWarnings({"unchecked", "rawtypes"})
@Slf4j
public class KafkaService {

  protected ExecutorService producerThreadPool = ThreadPoolUtil.newCachedLimitedThreadPool(PRODUCER_POOL);
  
  @Getter
  @Setter
  protected KafkaServiceConfig kafkaServiceConfig;
  
  protected Set<String> topicsStarted = new HashSet<>();
  
  @Getter
  protected Map<String, KafkaConsumerConfig<?,?>> topicKafkaConsumerConfigMap = new HashMap<>();

  protected Map<String, KafkaProducerConfig<?,?>> topicKafkaProducerConfigMap = new HashMap<>();

  @Getter
  private String configurationFileName;
  
  @Getter
  private Map<String, ConsumerThread> topicConsumerThreadMap = new HashMap<>();
  
  
  /**
   * A new KafkaService is constructed based on input filename.
   * 
   * @param classpath resource configuration file name
   */
  public KafkaService(String configFileName) {
    this.configurationFileName = configFileName;
    FinalizeStep finalizeStep = new FinalizeStep(this, null);
    GracefulShutdownStep gracefullShutdownStep = new GracefulShutdownStep(this, finalizeStep);
    ConstructionStep constructionStep = new ConstructionStep(this, gracefullShutdownStep);
    ValidationStep preValidationStep = new ValidationStep(this, constructionStep);
    ReadConfigurationStep readConfigurationService =
        new ReadConfigurationStep(this, preValidationStep);
    readConfigurationService.execute();
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
  protected void registerConsumer(String topic, ConsumerHandler consumerHandler) {
    validateTopicRegistration(topic, consumerHandler);

    KafkaConsumerConfig<?,?> consumerConfig = topicKafkaConsumerConfigMap.get(topic);

    long pollTime = Long.parseLong((String) consumerConfig.getProperties()
        .getOrDefault(MAX_POLL_INTERVAL_MS, DEFAULT_MAX_POLL_INTERVAL_MS));
    Duration pollDuration = Duration.ofMillis(pollTime);
    Integer partitionCount = Objects.isNull(consumerConfig.getPartitionCount()) ? DEFAULT_PARTITION_COUNT : consumerConfig.getPartitionCount();
    IntStream.range(0, partitionCount).forEach(i -> {
      String threadName = String.join(DASH,topic,String.valueOf(i));
      ConsumerThread consumerThread = new ConsumerThread();
      long maxProcessTime = pollDuration.toMillis()>DEFAULT_MAX_PROCESS_TIME ? DEFAULT_MAX_PROCESS_TIME : pollDuration.toMillis();
      consumerThread.initialize(this, consumerThread, consumerConfig, consumerHandler, pollDuration, threadName, maxProcessTime);
      topicConsumerThreadMap.computeIfAbsent(threadName, key -> {
        consumerThread.start();
        return consumerThread;
      });
    });
    topicsStarted.add(topic);
    if (Objects.nonNull(consumerConfig.getRetryPolicys())
        && !consumerConfig.getRetryPolicys().isEmpty()) {
      consumerConfig.getRetryPolicys().entrySet().forEach(rpEntry -> {
        KafkaConsumerConfig retryConsumerConfig =
            topicKafkaConsumerConfigMap.get(rpEntry.getValue().getRetryTopic());
        if(!topicsStarted.contains(retryConsumerConfig.getTopic())) {
          retryConsumerConfig.setMaxRetryCount(Objects.isNull(consumerConfig.getMaxRetryCount()) ? DEFAULT_MAX_RETRY_COUNT : consumerConfig.getMaxRetryCount());
          int retryPartitionCount = Objects.isNull(retryConsumerConfig.getPartitionCount()) ? DEFAULT_PARTITION_COUNT : retryConsumerConfig.getPartitionCount().intValue();
          Duration duration =  Duration.ofSeconds(rpEntry.getValue().getSeconds());
          IntStream.range(0, retryPartitionCount).forEach(i -> {
            String threadName = String.join(DASH,rpEntry.getValue().getRetryTopic() ,String.valueOf(i));
            ConsumerThread retryThread = new RetryThread();
            retryThread.initialize(this, retryThread, retryConsumerConfig, consumerHandler, duration, threadName, DEFAULT_MAX_PROCESS_TIME);
            topicConsumerThreadMap.computeIfAbsent(threadName,
                key -> {
                  retryThread.start();
                  return retryThread;
                });
          });
          topicsStarted.add(retryConsumerConfig.getTopic());
        }
      });

    }
    
//    waitPartitionAssignment();
  }
  
  protected boolean isBrokersAvailable() {
    Set<String> brokerIps = this.topicKafkaConsumerConfigMap.values().stream().map(config->((String)config.getProperties().get(BOOTSTRAP_SERVERS))).collect(Collectors.toSet());
    brokerIps.addAll(this.topicKafkaProducerConfigMap.values().stream().map(config->((String)config.getProperties().get(BOOTSTRAP_SERVERS))).collect(Collectors.toSet()));
    
    Set<String> allBrokerIps = new HashSet<>();
    brokerIps.stream().forEach(brokerIp->{
      allBrokerIps.addAll(Arrays.stream(brokerIp.split(",")).map(String::trim).collect(Collectors.toSet()));
    });
    int clientTimeoutMs = 5000;  
    try {
      allBrokerIps.stream().forEach(broker->{
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS, broker);
        try (AdminClient client = AdminClient.create(properties)) {
          client.listTopics(new ListTopicsOptions().timeoutMs(clientTimeoutMs)).listings().get();
        } catch (ExecutionException | InterruptedException ex) {
            log.error("Kafka is not available, timed out after {} ms", clientTimeoutMs);
            throw new KafkaRuntimeException(ex);
        }
      });
    } catch (Exception e) {
      return false;
    }
    return true;
  }

  protected boolean isRetryableErrorCode(String topic, String errorCode) {
    RetryPolicy retryPolicy = topicKafkaConsumerConfigMap.get(topic).getRetryPolicys().get(errorCode);
    return Objects.nonNull(retryPolicy);
  }

  protected UUID sendSync(String topic, Serializable message) {
    validateProducerRequest(topic, message);
    Producer producer = getProducerByTopic(topic);
    ProducerRecord record = createProducerRecord(topic, message);
    UUID uuid = UUID.randomUUID();
    addHeader(record,UNQUE_ID,uuid);
    try {
      producer.send(record).get();
    } catch (InterruptedException | ExecutionException e) {
      log.error("Error sending message ", e);
      throw new KafkaRuntimeException(e);
    }
    return uuid;
  }

  protected UUID sendAsync(String topic, Serializable message) {
    validateProducerRequest(topic, message);
    Producer producer = getProducerByTopic(topic);
    ProducerRecord record = createProducerRecord(topic, message);
    UUID uuid = UUID.randomUUID();
    addHeader(record,UNQUE_ID,uuid);
    this.producerThreadPool.submit(() ->producer.send(record));
    return uuid;
  }
  
  protected UUID sendAsyncWithCallback(String topic, Serializable message, Callback callback) {
    validateProducerRequest(topic, message);
    Producer producer = getProducerByTopic(topic);
    ProducerRecord record = createProducerRecord(topic, message);
    UUID uuid = UUID.randomUUID();
    addHeader(record,UNQUE_ID,uuid);
    producerThreadPool.submit(() -> producer.send(record,callback));
    return uuid;
  }
  
  protected void sendAsync(String topic,ProducerRecord record) {
    Producer producer = getProducerByTopic(topic);
    this.producerThreadPool.submit(() ->producer.send(record));
  }
  
  private Producer<?, ?> getProducerByTopic(String topic) {
    return topicKafkaProducerConfigMap.get(topic).getKafkaProducer();
  }
  
  protected ProducerRecord<?,?> createProducerRecord(String topic,Object message){
    KafkaProducerConfig config = topicKafkaProducerConfigMap.get(topic);
    if(config.isValueSerDesByteArray()) {
      return new ProducerRecord(topic,SerializationUtils.serialize((Serializable)message));
    }
    return new ProducerRecord(topic, message);
  }

  private void addHeader(ProducerRecord<?,?> record,String key,Serializable value) {
    if(Objects.isNull(value)) {
      return;
    }
    record.headers().add(key,SerializationUtils.serialize(value));
  }

  private void validateProducerRequest(String topic, Serializable message) {
    if (Objects.isNull(topicKafkaProducerConfigMap.get(topic))) {
      log.error("Topic {} not found in {}. Please define a producer configuration..\n{}",topic,this.configurationFileName,SAMPLE_PRODUCER_CONSUMER_CONFIGURATION);
      throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,
          "Topic not found in kafka-config.json. Please define a producer configuration..\n"+SAMPLE_PRODUCER_CONSUMER_CONFIGURATION);
    }
    if(Objects.isNull(message)) {
    	throw new KafkaRuntimeException(Errors.INVALID_REQUEST_ERROR,
    	          "Message can not be null");
    }
  }
  
  private void validateTopicRegistration(String topic, ConsumerHandler consumerHandler) {
    if (topicsStarted.contains(topic)) {
      log.warn("Topic {} already started. Ignoring request.", topic);
      throw new KafkaRuntimeException(Errors.INVALID_REQUEST_ERROR,
          "Topic already started");
    }
    if (Objects.isNull(topic)) {
      log.error("Topic can not be null to register");
      throw new KafkaRuntimeException(Errors.INVALID_REQUEST_ERROR,
          "Topic to register can not be null ");
    }
    if (Objects.isNull(consumerHandler)) {
      log.error("consumerHandler to register can not be null");
      throw new KafkaRuntimeException(Errors.INVALID_REQUEST_ERROR,
          "ConsumerHandler can not be null to register");
    }
    
    if(Objects.isNull(topicKafkaConsumerConfigMap.get(topic))) {
      log.error("There is no consumer topic {} in {} file",topic,this.configurationFileName);
      throw new KafkaRuntimeException(Errors.INVALID_REQUEST_ERROR,"There is no consumer with topic "+topic+" in "+this.configurationFileName+" file");
    }
  }
  

  private void waitTopicPartitionAssignment(ConsumerThread consumerThread,
      Map<String,AtomicBoolean> topicFlagMap) {
    KafkaConsumerConfig<?, ?> config = consumerThread.getConsumerConfig();
    AtomicBoolean flag = topicFlagMap.get(config.getTopic());
    while (!flag.get()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.error("Interrupt Excetion", e);
        break;
      }
      if(!consumerThread.getAssignedPartitions().isEmpty()) {
        flag.set(true);
        log.debug("Topic {} has partitions assignment done", config.getTopic());
        break;
      }
    }
  }
  
  @SuppressWarnings("unused")
  private void waitPartitionAssignment() {
    ConcurrentHashMap<String,AtomicBoolean> topicFlagMap = new ConcurrentHashMap<>();
    topicsStarted.stream().forEach(topic->topicFlagMap.put(topic, new AtomicBoolean()));
    List<Future<?>> futures = new ArrayList<>();
    ExecutorService executorService = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());
    for(ConsumerThread consumerThread : topicConsumerThreadMap.values()) {
      futures.add(executorService.submit(()->{
        waitTopicPartitionAssignment(consumerThread,topicFlagMap);
      }));
    }
    for(Future future:futures) {
      try {
        future.get();
      } catch (InterruptedException|ExecutionException e) {
        log.error("Error getting future",e);
      }
    }
    executorService.shutdown();
  }
  
  protected void close(long timeout) {
    log.info("Closing producer executor service now..");
    GracefulShutdownStep.shutdownAndAwaitTermination(producerThreadPool);
    topicKafkaProducerConfigMap.values().forEach(producerConfig->{
      log.info("Flushing final transactions for producer topic {}",producerConfig.getTopic());
      getProducerByTopic(producerConfig.getTopic()).flush();
      getProducerByTopic(producerConfig.getTopic()).close(Duration.ofMillis(timeout));
    });
  }
  
}
