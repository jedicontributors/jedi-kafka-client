package jedi.kafka.model;

import java.util.concurrent.atomic.AtomicLong;

import jedi.kafka.service.ConsumerHandler;
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
}