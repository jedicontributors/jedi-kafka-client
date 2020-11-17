package jedi.kafka.model;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;

import com.google.gson.annotations.Expose;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
public class KafkaServiceConfig {

  @Expose
  private Map<String, KafkaProducerConfig<?,?>> producers = new HashMap<>();

  @Expose
  private Map<String, KafkaConsumerConfig<?,?>> consumers = new HashMap<>();


  @Getter
  @Setter
  public class KafkaConfig {

    @Expose
    private String topic;

    @Expose
    private Map<String, Object> properties;
    
    private boolean isValueSerDesByteArray;
    
  }

  @Getter
  @Setter
  @NoArgsConstructor
  public class KafkaProducerConfig<K,V> extends KafkaConfig {
    private KafkaProducer<String,?> kafkaProducer;

    public KafkaProducerConfig(KafkaProducerConfig<K,V> kafkaProducerConfig) {
      Map<String, Object> newProperties = new HashMap<>();
      newProperties.putAll(kafkaProducerConfig.getProperties());
      this.setProperties(newProperties);
    }
  }

  @Getter
  @Setter
  @NoArgsConstructor
  public class KafkaConsumerConfig<K,V> extends KafkaConfig {
    @Expose
    private Integer maxRetryCount;

    @Expose
    private Integer partitionCount;

    @Expose
    private Long processTimeout;

    @Expose
    private Map<String, RetryPolicy> retryPolicys;
    
    @Expose
    private Integer maximumThreadCount;
    
    public KafkaConsumerConfig(KafkaConsumerConfig<K,V> kafkaConsumerConfig) {
      Map<String, Object> newProperties = new HashMap<>();
      newProperties.putAll(kafkaConsumerConfig.getProperties());
      this.setProperties(newProperties);
      this.setMaxRetryCount(kafkaConsumerConfig.maxRetryCount);
      this.setPartitionCount(kafkaConsumerConfig.partitionCount);
      this.setRetryPolicys(new HashMap<>());
    }
  }

  public KafkaProducerConfig<?,?> createProducer(String name) {
    KafkaProducerConfig<?,?> config = new KafkaProducerConfig<>();
    this.producers.put(name, config);
    return config;
  }

}
