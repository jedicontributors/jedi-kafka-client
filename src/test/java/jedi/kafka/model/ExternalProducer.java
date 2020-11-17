package jedi.kafka.model;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import jedi.kafka.util.ThreadPoolUtil;

public class ExternalProducer extends Thread{
  
  private KafkaProducer<?,?> producer;
  
  private String topic;
  
  private String brokerIps;
  
  private String serializerClassName;
  
  private ExecutorService producerThreadPool = ThreadPoolUtil.newCachedLimitedThreadPool("ExternalProducer");

  public ExternalProducer(String topic, String brokerIps,String serializerClassName) {
    super();
    this.topic = topic;
    this.brokerIps = brokerIps;
    this.serializerClassName = serializerClassName;
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void run() {
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerIps);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,serializerClassName);
    producer = new KafkaProducer<>(properties);
    IntStream.range(0, 10).forEach(item->{
      producerThreadPool.submit(()->{
      if(ByteArraySerializer.class.getName().equals(serializerClassName)) {
        producer.send(new ProducerRecord(topic, SerializationUtils.serialize(item)));
      }else {
        producer.send(new ProducerRecord(topic, item));
      }
      });
    });
  }
  
}
