package jedi.kafka.model;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExternalConsumer extends Thread{

  private KafkaConsumer<?,?> consumer;
  
  private String topic;
  
  private String brokerIps;
  
  private boolean isShutdownInprogress;
  
  private String deserializerClassName;
  
  public ExternalConsumer(String topic,String brokerIps,String deserializerClassName){
    this.topic = topic;
    this.brokerIps = brokerIps;
    this.deserializerClassName = deserializerClassName;
  }
  
  @Override
  public void run() {
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerIps);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        StringDeserializer.class.getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,deserializerClassName);
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,  topic+"-grp");
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    properties.setProperty("max.poll.interval.ms", "3000");
    consumer = new KafkaConsumer<>(properties);
    consumer.subscribe(Arrays.asList(topic));
    while (!isShutdownInprogress) {
      try {
        ConsumerRecords<?,?> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<?,?> record : records) {
          StringBuilder builder = new StringBuilder();
          if(ByteArrayDeserializer.class.getName().equals(deserializerClassName)) {
            builder.append(" record: ").append(SerializationUtils.deserialize((byte[])record.value()).toString());
          }else {
            builder.append(" record: ").append(record.value().toString());
          }
          for(Header header:record.headers()) {
            builder.append(" ").append(header.key()).append(": ").append(SerializationUtils.deserialize(header.value()).toString());
          }
          log.debug(builder.toString());
        }
      } catch (Exception e) {
        consumer.close();
      }
    }  
    log.info("Closing consumer");
    consumer.close();
  }
  
  public void close() {
    this.isShutdownInprogress=true;
  }
}
