package jedi.kafka.model;

import org.apache.commons.lang3.SerializationUtils;

import jedi.kafka.service.ConsumerHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TestByteArrayConsumer implements ConsumerHandler<byte[]> {
  @Override
  public Response onMessage(byte[] message) {
    log.debug("Recieved message {} after deserialization",SerializationUtils.deserialize(message).toString());
    return Response.getSuccessResult();
  }
}
