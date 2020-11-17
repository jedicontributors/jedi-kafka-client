package jedi.kafka.unit;

import static jedi.kafka.model.TestConstants.EMPTY_STRING;
import static jedi.kafka.model.TestConstants.ERROR_CODE_1;
import static jedi.kafka.model.TestConstants.PRODUCER;
import static jedi.kafka.model.TestConstants.RETRY;
import static jedi.kafka.model.TestConstants.RETRY_10L;
import static jedi.kafka.model.TestConstants.RETRY_TOPIC;
import static jedi.kafka.model.TestConstants.TEST;
import static jedi.kafka.model.TestConstants.TEST_TOPIC;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import jedi.kafka.model.KafkaRuntimeException;
import jedi.kafka.model.KafkaServiceConfig;
import jedi.kafka.model.RetryPolicy;
import jedi.kafka.model.KafkaServiceConfig.KafkaProducerConfig;
import jedi.kafka.service.ValidationStep;

@RunWith(MockitoJUnitRunner.class)
public class PreValidationStepTest extends StepTest<ValidationStep>{
  
  private ValidationStep validationStep;
  
  public PreValidationStepTest() {
    super(ValidationStep.class);
    validationStep = getStep();
  }

  @Test
  public void test_empty_features_in_kafkaConfig() throws Exception  {
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_producer_empty_topic_kafkaConfig() {
    createWithProducer();
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_producer_nil_properties_kafkaConfig() {
    createWithProducer();
    validationStep.process();
  }

  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_nil_features() {
    createWithConsumer();
    validationStep.process();
  }

  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_topic_notEmpty_property_is_nil() {
    createWithConsumer();
    addTopic();
    validationStep.process();
  }
  
  @Test
  public void test_consumer_property_procesTime_is_nil() throws Exception {
    createWithConsumer();
    addTopicWithProperty();
//    PowerMockito.when(kafkaService, "getKafkaServiceConfig").thenReturn(kafkaServiceConfig);
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_property_procesTime_equals_zero() {
    createWithConsumer();
    addTopicWithProperty();
    kafkaServiceConfig.getConsumers().get(TEST).setProcessTimeout(Long.valueOf(0));
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_retry_policy_nil_fields() {
    createWithConsumer();
    addTopicWithProperty();
    Map<String,RetryPolicy> retryPolicy = new HashMap<>();
    retryPolicy.put(RETRY, new RetryPolicy());
    kafkaServiceConfig.getConsumers().get(TEST).setRetryPolicys(retryPolicy);
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_retry_policy_retry_topic_is_nil() {
    createWithConsumer();
    addTopicWithProperty();
    Map<String,RetryPolicy> retryPolicyMap = new HashMap<>();
    RetryPolicy retryPolicy =  new RetryPolicy();
    retryPolicy.setErrorCode(ERROR_CODE_1);
    retryPolicy.setSeconds(Long.valueOf(1));
    retryPolicyMap.put(RETRY,retryPolicy);
    kafkaServiceConfig.getConsumers().get(TEST).setRetryPolicys(retryPolicyMap);
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_retry_policy_retry_topic_is_empty() {
    createWithConsumer();
    addTopicWithProperty();
    Map<String,RetryPolicy> retryPolicyMap = new HashMap<>();
    RetryPolicy retryPolicy =  new RetryPolicy();
    retryPolicy.setErrorCode(ERROR_CODE_1);
    retryPolicy.setSeconds(Long.valueOf(1));
    retryPolicy.setRetryTopic(EMPTY_STRING);
    retryPolicyMap.put(RETRY,retryPolicy);
    kafkaServiceConfig.getConsumers().get(TEST).setRetryPolicys(retryPolicyMap);
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_retry_policy_retry_seconds_is_nil() {
    createWithConsumer();
    addTopicWithProperty();
    Map<String,RetryPolicy> retryPolicyMap = new HashMap<>();
    RetryPolicy retryPolicy =  new RetryPolicy();
    retryPolicy.setRetryTopic(RETRY_TOPIC);
    retryPolicy.setErrorCode(ERROR_CODE_1);
    retryPolicyMap.put(RETRY,retryPolicy);
    kafkaServiceConfig.getConsumers().get(TEST).setRetryPolicys(retryPolicyMap);
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_retry_policy_retry_seconds_is_zero() {
    createWithConsumer();
    addTopicWithProperty();
    Map<String,RetryPolicy> retryPolicyMap = new HashMap<>();
    RetryPolicy retryPolicy =  new RetryPolicy();
    retryPolicy.setRetryTopic(RETRY_TOPIC);
    retryPolicy.setErrorCode(ERROR_CODE_1);
    retryPolicy.setSeconds(Long.valueOf(0));
    retryPolicyMap.put(RETRY,retryPolicy);
    kafkaServiceConfig.getConsumers().get(TEST).setRetryPolicys(retryPolicyMap);
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_retry_policy_retry_errorCode_is_nil() {
    createWithConsumer();
    addTopicWithProperty();
    Map<String,RetryPolicy> retryPolicyMap = new HashMap<>();
    RetryPolicy retryPolicy =  new RetryPolicy();
    retryPolicy.setRetryTopic(RETRY_TOPIC);
    retryPolicy.setSeconds(10L);
    retryPolicyMap.put(RETRY,retryPolicy);
    kafkaServiceConfig.getConsumers().get(TEST).setRetryPolicys(retryPolicyMap);
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_retry_policy_retry_errorCode_is_empty() {
    createWithConsumer();
    addTopicWithProperty();
    Map<String,RetryPolicy> retryPolicyMap = new HashMap<>();
    RetryPolicy retryPolicy =  new RetryPolicy();
    retryPolicy.setRetryTopic(RETRY_TOPIC);
    retryPolicy.setSeconds(10L);
    retryPolicy.setErrorCode(EMPTY_STRING);
    retryPolicyMap.put(RETRY,retryPolicy);
    kafkaServiceConfig.getConsumers().get(TEST).setRetryPolicys(retryPolicyMap);
    validationStep.process();
  }
  
  @Test(expected=KafkaRuntimeException.class)
  public void test_consumer_retry_policy_retry_topic_equals_consumer_topic() {
    createWithConsumer();
    addTopicWithProperty();
    Map<String,RetryPolicy> retryPolicyMap = new HashMap<>();
    RetryPolicy retryPolicy =  new RetryPolicy();
    retryPolicy.setRetryTopic(TEST_TOPIC);
    retryPolicy.setErrorCode(ERROR_CODE_1);
    retryPolicy.setSeconds(RETRY_10L);
    retryPolicyMap.put(RETRY,retryPolicy);
    kafkaServiceConfig.getConsumers().get(TEST).setRetryPolicys(retryPolicyMap);
    validationStep.process();
  }

  private void addTopic() {
    kafkaServiceConfig.getConsumers().get(TEST).setTopic(TEST_TOPIC);
  }
  
  private void addTopicWithProperty() {
    kafkaServiceConfig.getConsumers().get(TEST).setTopic(TEST_TOPIC);
    kafkaServiceConfig.getConsumers().get(TEST).setProperties(new HashMap<>());
  }
  
  private void createWithProducer() {
    KafkaProducerConfig<?,?> kafkaProducerConfig= kafkaServiceConfig.createProducer(PRODUCER);
    kafkaServiceConfig.getProducers().put(TEST, kafkaProducerConfig);
  }
  
  private void createWithConsumer() {
    KafkaServiceConfig.KafkaConsumerConfig<?,?> consumer = kafkaServiceConfig.new KafkaConsumerConfig<>();
    kafkaServiceConfig.getConsumers().put(TEST, consumer);
  }
}
