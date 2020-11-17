package jedi.kafka.model;

import java.io.Serializable;
import java.util.UUID;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class KafkaMessage<T> implements Serializable{
  
  private static final long serialVersionUID = -7395207405472468001L;
  private UUID uniqueId;
  private T message;
  private Integer retryCounter;
  private Long nextRetryTimestamp;
  
  public KafkaMessage(T message) {
    this.message = message;
  }
  
  public T getMessage() {
    return message;
  }
}
