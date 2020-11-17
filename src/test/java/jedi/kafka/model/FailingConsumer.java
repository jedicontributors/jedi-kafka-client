package jedi.kafka.model;

import jedi.kafka.service.ConsumerHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FailingConsumer implements ConsumerHandler<String> {

  @Override
  public Response onMessage(String message) {
    log.debug("Recieved message {}",message);
//    try {
//      Thread.currentThread().join(3000L);
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }
    return new Response(-1, "failed");
  }

}
