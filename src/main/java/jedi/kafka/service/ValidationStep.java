package jedi.kafka.service;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import jedi.kafka.model.Errors;
import jedi.kafka.model.KafkaConstants;
import jedi.kafka.model.KafkaRuntimeException;
import jedi.kafka.model.KafkaServiceConfig;
import jedi.kafka.model.RetryPolicy;
import jedi.kafka.model.Step;
import jedi.kafka.model.StepDetails;
import jedi.kafka.model.KafkaServiceConfig.KafkaConfig;
import jedi.kafka.model.KafkaServiceConfig.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ValidationStep extends Step {

  public ValidationStep(KafkaService kafkaService,Step step) {
    super(kafkaService,step);
  }

  @Override
  public void process() {
    KafkaServiceConfig kafkaServiceConfig = kafkaService.kafkaServiceConfig;
    if(Objects.isNull(kafkaServiceConfig)) {
      log.error("KafkaServiceConfig is nil");
      throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,"KafkaServiceConfig can not be nil");
    }
    validateProducers(kafkaServiceConfig);
    validateConsumers(kafkaServiceConfig);
  }
  
  @Override
  public StepDetails getServiceStep() {
    return StepDetails.PRE_VALIDATION;
  }
  
  private void validateProducers(KafkaServiceConfig kafkaServiceConfig) {
    if(Objects.nonNull(kafkaServiceConfig.getProducers())) {
      kafkaServiceConfig.getProducers().entrySet().forEach(entry->validateCommonProperties(entry.getKey(),entry.getValue()));
    }
  }
  
  
  private void validateConsumers(KafkaServiceConfig kafkaServiceConfig) {
    if(Objects.nonNull(kafkaServiceConfig.getConsumers())) {
      kafkaServiceConfig.getConsumers().entrySet().forEach(consumerEntry->{
        String consumerName = consumerEntry.getKey();
        KafkaConsumerConfig<?,?> kafkaConsumerConfig = consumerEntry.getValue();
        log.debug("Validating common properties for consumer {}",consumerName);
        validateCommonProperties(consumerName,kafkaConsumerConfig);
        checkEqualsZero(kafkaConsumerConfig.getProcessTimeout(), consumerName+".processTimeout can not be equal to 0");
        if(Objects.nonNull(kafkaConsumerConfig.getRetryPolicys())) {
          kafkaConsumerConfig.getRetryPolicys().values().stream().forEach(retryPolicy->{
            String retryPolicyName = consumerEntry.getKey();
            checkRetryTopicEqualsParentConsumerTopic(kafkaConsumerConfig, retryPolicy);
            String retryTopic = retryPolicy.getRetryTopic();
            checkNull(retryTopic, String.join(StringUtils.EMPTY,retryPolicyName,".retryPolicy.retryTopic must be declared"));
            checkNonEmptyString(retryTopic, String.join(StringUtils.EMPTY,retryPolicyName,".retryTopic can not be empty"));
            String errorCode = retryPolicy.getErrorCode();
            checkNull(errorCode, String.join(StringUtils.EMPTY,retryPolicyName,".retryPolicy.errorCode must be declared"));
            checkNonEmptyString(errorCode, String.join(StringUtils.EMPTY,retryPolicyName,".retryPolicy.errorCode can not have empty value"));
            Long seconds = retryPolicy.getSeconds();
            checkNull(seconds, String.join(StringUtils.EMPTY,retryPolicyName,".retryPolicy.duration in seconds must be declared"));
            checkEqualsZero(seconds, String.join(StringUtils.EMPTY,retryPolicyName,".retryPolicy.duration must be greater than 0"));
          });
        }
      });
    }
  }

  private void checkRetryTopicEqualsParentConsumerTopic(KafkaConsumerConfig<?,?> consumer,RetryPolicy retryPolicy) {
    if(consumer.getTopic().equals(retryPolicy.getRetryTopic())) {
      log.error("RetryTopic {} can not be same the same of parent topic {}",retryPolicy.getRetryTopic(),consumer.getTopic());
      throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,"RetryTopic "+retryPolicy.getRetryTopic()+" can not be same the same of parent topic "+consumer.getTopic());
    }
  }
  
  private void checkEqualsZero(Long value,String message) {
    if(Objects.nonNull(value) && value==0) {
      log.error(message);
      throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,message);
    }
  }
  
  private void checkNonEmptyString(String field,String message) {
    if(Objects.nonNull(field) && field.isEmpty()) {
      log.error(message);
      throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,message);
    }
  }
  
  private void checkNull(Object configuration,String message) {
    if(Objects.isNull(configuration)) {
      log.error(message);
      throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,message);
    }
  }
  
  private void validateCommonProperties(String configName,KafkaConfig config) {
    try {
      checkNonEmptyString(configName,"Producer/Consumer must have unique non-null non-empty alias");
      checkNull(config,"Missing configuration for "+configName);
      checkNonEmptyString(config.getTopic(),"Configuration "+configName+ " must have a topic defined");
      checkNull(config.getProperties(),"Configuration properties for "+configName+" must be declared");
    } catch (KafkaRuntimeException e) {
      logSampleProducerConsumer();
      throw e;
    }
  }

  private static void logSampleProducerConsumer() {
    log.error(KafkaConstants.SAMPLE_PRODUCER_CONSUMER_CONFIGURATION);
  }
}
