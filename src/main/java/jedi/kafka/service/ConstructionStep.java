package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.BOOTSTRAP_SERVERS;
import static jedi.kafka.model.KafkaConstants.BYTE_ARRAY_DESERIALIZER;
import static jedi.kafka.model.KafkaConstants.BYTE_ARRAY_SERIALIZER;
import static jedi.kafka.model.KafkaConstants.DASH;
import static jedi.kafka.model.KafkaConstants.DEFAULT_MAX_POLL_RECORDS;
import static jedi.kafka.model.KafkaConstants.DEFAULT_MAX_PROCESS_TIME;
import static jedi.kafka.model.KafkaConstants.DEFAULT_RECONNECT_BACKOFF_MAX_MS;
import static jedi.kafka.model.KafkaConstants.DEFAULT_RECONNECT_BACKOFF_MS;
import static jedi.kafka.model.KafkaConstants.DEFAULT_SESSION_TIMEOUT_MS;
import static jedi.kafka.model.KafkaConstants.ENABLE_AUTO_COMMIT;
import static jedi.kafka.model.KafkaConstants.GROUP;
import static jedi.kafka.model.KafkaConstants.GROUP_ID;
import static jedi.kafka.model.KafkaConstants.KEY_DESERIALIZER;
import static jedi.kafka.model.KafkaConstants.KEY_SERIALIZER;
import static jedi.kafka.model.KafkaConstants.MAX_POLL_INTERVAL_MS;
import static jedi.kafka.model.KafkaConstants.MAX_POLL_RECORDS;
import static jedi.kafka.model.KafkaConstants.MAX_PROCESS_TIME_SECONDS;
import static jedi.kafka.model.KafkaConstants.RECONNECT_BACKOFF_MAX_MS;
import static jedi.kafka.model.KafkaConstants.RECONNECT_BACKOFF_MS;
import static jedi.kafka.model.KafkaConstants.REQUEST_TIMEOUT_MS;
import static jedi.kafka.model.KafkaConstants.SECOND_TO_MILISECOND_COEFFICIENT;
import static jedi.kafka.model.KafkaConstants.SESSION_TIMEOUT_MS;
import static jedi.kafka.model.KafkaConstants.STRING_DESERIALIZER;
import static jedi.kafka.model.KafkaConstants.STRING_SERIALIZER;
import static jedi.kafka.model.KafkaConstants.VALUE_DESERIALIZER;
import static jedi.kafka.model.KafkaConstants.VALUE_SERIALIZER;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jedi.kafka.model.KafkaServiceConfig;
import jedi.kafka.model.KafkaServiceConfig.KafkaConsumerConfig;
import jedi.kafka.model.KafkaServiceConfig.KafkaProducerConfig;
import jedi.kafka.model.RetryPolicy;
import jedi.kafka.model.Step;
import jedi.kafka.model.StepDetails;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConstructionStep extends Step {

  public ConstructionStep(KafkaService kafkaService, Step serviceChain) {
    super(kafkaService, serviceChain);
  }

  @Override
  public StepDetails getServiceStep() {
    return StepDetails.CONSTRUCTION;
  }

  @Override
  public void process() {
    addMissingConfigurations();
    constructProducers();
    constructConsumers();
  }

  private void addMissingConfigurations() {
    Map<String, KafkaProducerConfig<?,?>> tempProducerMap = new HashMap<>();
    Map<String, KafkaConsumerConfig<?,?>> tempConsumerMap = new HashMap<>();
    KafkaServiceConfig kafkaServiceConfig = kafkaService.getKafkaServiceConfig();
    kafkaServiceConfig.getConsumers().entrySet().stream().forEach(entry -> {
      KafkaConsumerConfig<?,?> consumer = entry.getValue();
      if (Objects.nonNull(consumer.getRetryPolicys())) {
        KafkaServiceConfig serviceConfig = new KafkaServiceConfig();
        List<Map<String, RetryPolicy>> retryPolicyList = new ArrayList<>();
        consumer.getRetryPolicys().values().stream().forEach(retryPolicy -> {
          Optional<KafkaConsumerConfig<?,?>> definedRetryConsumerDefinition =
              kafkaServiceConfig.getConsumers().values().stream()
                  .filter(kcc -> retryPolicy.getRetryTopic().equals(kcc.getTopic())).findFirst();
          createRetryConsumer(tempConsumerMap, consumer, serviceConfig, retryPolicyList,
              retryPolicy, definedRetryConsumerDefinition);

          Optional<KafkaProducerConfig<?,?>> existingRetryProducer =
              kafkaServiceConfig.getProducers().values().stream()
                  .filter(kcc -> retryPolicy.getRetryTopic().equals(kcc.getTopic())).findFirst();

          createRetryProducer(tempProducerMap, kafkaServiceConfig, consumer, serviceConfig,
              retryPolicy, existingRetryProducer);
        });
        consumer.setRetryPolicys(new HashMap<>());
        retryPolicyList.forEach(map -> consumer.getRetryPolicys().putAll(map));
      }
    });
    if(!tempProducerMap.isEmpty()) {
      kafkaServiceConfig.getProducers().putAll(tempProducerMap);
    }
    if(!tempConsumerMap.isEmpty()) {
      kafkaServiceConfig.getConsumers().putAll(tempConsumerMap);
    }
  }

  private void createRetryProducer(Map<String, KafkaProducerConfig<?,?>> tempProducerMap,
      KafkaServiceConfig kafkaServiceConfig, KafkaConsumerConfig<?,?> consumer,
      KafkaServiceConfig serviceConfig, RetryPolicy retryPolicy,
      Optional<KafkaProducerConfig<?,?>> existingRetryProducer) {
    if (Optional.empty().equals(existingRetryProducer)) {
      Optional<KafkaProducerConfig<?,?>> consumerProducer =
          kafkaServiceConfig.getProducers().values().stream()
              .filter(kcc -> consumer.getTopic().equals(kcc.getTopic())).findFirst();
      if (Optional.empty().equals(consumerProducer)) {
        KafkaProducerConfig<?,?> newProducerConfig =
            kafkaServiceConfig.createProducer(retryPolicy.getRetryTopic());
        newProducerConfig.setTopic(retryPolicy.getRetryTopic());
        newProducerConfig.setProperties(new HashMap<>());
        newProducerConfig.getProperties().put(BOOTSTRAP_SERVERS,
            consumer.getProperties().get(BOOTSTRAP_SERVERS));
        tempProducerMap.put(retryPolicy.getRetryTopic(), newProducerConfig);
      } else if(consumerProducer.isPresent()){
        log.info("inherited from producer {}, creating a producer for retryPolicy topic {}",
            consumer.getTopic(), retryPolicy.getRetryTopic());
        KafkaProducerConfig<?,?> newProducerConfig = serviceConfig.new KafkaProducerConfig<>(consumerProducer.get());
        newProducerConfig.setTopic(retryPolicy.getRetryTopic());
        tempProducerMap.put(retryPolicy.getRetryTopic(), newProducerConfig);
      }
    }
  }

  private void createRetryConsumer(Map<String, KafkaConsumerConfig<?,?>> tempConsumerMap,
      KafkaConsumerConfig<?,?> consumer, KafkaServiceConfig serviceConfig,
      List<Map<String, RetryPolicy>> retryPolicyList, RetryPolicy retryPolicy,
      Optional<KafkaConsumerConfig<?,?>> definedRetryConsumerDefinition) {
    if (Optional.empty().equals(definedRetryConsumerDefinition)) {
      log.info("Inherited from consumer {}, creating a consumer for retryPolicy topic {}",
          consumer.getTopic(), retryPolicy.getRetryTopic());
      KafkaConsumerConfig<?,?> newRetryConsumer = serviceConfig.new KafkaConsumerConfig<>(consumer);
      newRetryConsumer.setTopic(retryPolicy.getRetryTopic());
      long maxRetryDuration = retryPolicy.getSeconds() * SECOND_TO_MILISECOND_COEFFICIENT;
      if(maxRetryDuration<DEFAULT_MAX_PROCESS_TIME) {
        maxRetryDuration = maxRetryDuration+DEFAULT_MAX_PROCESS_TIME;
      }
      newRetryConsumer.getProperties().put(MAX_POLL_INTERVAL_MS,String.valueOf(maxRetryDuration));
      newRetryConsumer.getProperties().put(REQUEST_TIMEOUT_MS,String.valueOf(maxRetryDuration));
      newRetryConsumer.getProperties().put(GROUP_ID, String.join(DASH,retryPolicy.getRetryTopic(),GROUP));
      if (retryPolicy.getSeconds().longValue() >= MAX_PROCESS_TIME_SECONDS) {
        newRetryConsumer.getProperties().put(SESSION_TIMEOUT_MS, DEFAULT_SESSION_TIMEOUT_MS);
      }

      String[] errorCodes = retryPolicy.getErrorCode().split(",");
      Map<String, RetryPolicy> retryTopicErrorCodePolicyMap = new HashMap<>();
      Arrays.asList(errorCodes).forEach(errorcode -> {
        RetryPolicy policy = new RetryPolicy();
        policy.setErrorCode(errorcode);
        policy.setRetryTopic(retryPolicy.getRetryTopic());
        policy.setSeconds(retryPolicy.getSeconds());
        retryTopicErrorCodePolicyMap.put(errorcode, policy);
      });
      newRetryConsumer.getRetryPolicys().putAll(retryTopicErrorCodePolicyMap);
      retryPolicyList.add(retryTopicErrorCodePolicyMap);
      tempConsumerMap.put(retryPolicy.getRetryTopic(), newRetryConsumer);
    }
  }



  private void constructProducers() {
    kafkaService.kafkaServiceConfig.getProducers().values().forEach(kp -> 
      kafkaService.topicKafkaProducerConfigMap.computeIfAbsent(kp.getTopic(), key -> {
        Map<String, Object> props = kp.getProperties();
        props.computeIfAbsent(KEY_SERIALIZER, keySerializer -> STRING_SERIALIZER);
        props.computeIfAbsent(VALUE_SERIALIZER, valueSerializer -> BYTE_ARRAY_SERIALIZER);
        if(props.get(VALUE_SERIALIZER).equals(BYTE_ARRAY_SERIALIZER)) {
          kp.setValueSerDesByteArray(true);
        }
        props.computeIfAbsent(RECONNECT_BACKOFF_MS, backOffMs -> DEFAULT_RECONNECT_BACKOFF_MS);
        props.computeIfAbsent(RECONNECT_BACKOFF_MAX_MS, backOffMax -> DEFAULT_RECONNECT_BACKOFF_MAX_MS);
        log.info("Creating producer for topic {}", kp.getTopic());
        return kp;
      })
    );
  }

  private void constructConsumers() {
    kafkaService.kafkaServiceConfig.getConsumers().values().forEach(kcc ->
      kafkaService.topicKafkaConsumerConfigMap.computeIfAbsent(kcc.getTopic(), key -> {
        Map<String, Object> props = kcc.getProperties();
        props.computeIfAbsent(KEY_DESERIALIZER, keySerializer -> STRING_DESERIALIZER); 
        if(Objects.nonNull(props.get(VALUE_DESERIALIZER)) && props.get(VALUE_DESERIALIZER).equals(BYTE_ARRAY_DESERIALIZER)) {
          kcc.setValueSerDesByteArray(true);
        }
        props.computeIfAbsent(VALUE_DESERIALIZER, keyDeserializer -> BYTE_ARRAY_DESERIALIZER);
        props.computeIfAbsent(ENABLE_AUTO_COMMIT, autoCommit -> String.valueOf(Boolean.FALSE));
        props.computeIfAbsent(RECONNECT_BACKOFF_MS, backOffMs -> DEFAULT_RECONNECT_BACKOFF_MS);
        props.computeIfAbsent(RECONNECT_BACKOFF_MAX_MS, backOffMax -> DEFAULT_RECONNECT_BACKOFF_MAX_MS);
        props.computeIfAbsent(MAX_POLL_RECORDS, maxPollRecords -> DEFAULT_MAX_POLL_RECORDS);
        props.computeIfAbsent(SESSION_TIMEOUT_MS, sessionTimeoutMs -> DEFAULT_SESSION_TIMEOUT_MS);
        props.computeIfAbsent(GROUP_ID, groupId -> String.join(DASH, kcc.getTopic(), GROUP));
        return kcc;
      })
    );
  }
}
