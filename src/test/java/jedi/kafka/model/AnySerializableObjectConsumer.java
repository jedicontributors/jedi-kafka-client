package jedi.kafka.model;

import java.util.concurrent.atomic.AtomicLong;

import jedi.kafka.service.ConsumerHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AnySerializableObjectConsumer implements ConsumerHandler<AnySerializableObject> {

  private AtomicLong counter = new AtomicLong();
  
  @Override
  public Response onMessage(AnySerializableObject message) {
    log.debug("{} - Recieved {}",counter.incrementAndGet(),message);
    return Response.getSuccessResult();
  }

}
