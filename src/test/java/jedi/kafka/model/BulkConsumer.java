package jedi.kafka.model;

import java.util.List;

import jedi.kafka.service.ConsumerHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BulkConsumer implements ConsumerHandler<List<String>>{

  @Override
  public boolean isBulkConsumer() {
    return true;
  }
  
  @Override
  public Response onMessage(List<String> message) {
    log.debug("Recieved messages size of {} {}",message.size(),message.toString());
    return Response.getSuccessResult();
  }
  
}