package jedi.kafka.model;

import java.util.concurrent.atomic.AtomicLong;

import jedi.kafka.service.ConsumerHandler;
import jedi.kafka.service.JediKafkaClient;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StringConsumer implements ConsumerHandler<String> {
  
  @Getter
  private AtomicLong counter = new AtomicLong();
  @Setter
  private Response response;
  
  @Override
  public Response onMessage(String message) {
    log.debug(counter.incrementAndGet()+"-Recieved message "+message);
    return response;
  }
  
  public static void main(String[] args) {
    JediKafkaClient kafkaClient = JediKafkaClient.getInstance("test-string-deserializer-consumer.json");
    kafkaClient.registerConsumer("test-string", new StringConsumer());
  }
  
}