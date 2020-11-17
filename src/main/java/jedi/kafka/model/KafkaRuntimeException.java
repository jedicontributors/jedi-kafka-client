package jedi.kafka.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
@Getter
@Setter
@ToString
public class KafkaRuntimeException extends RuntimeException {

  private static final String DASH = "-";
  private static final long serialVersionUID = -5542844793000740064L;
  private final String errorCode;
  private final String errorDescription;

  public KafkaRuntimeException(Throwable throwable) {
    super(throwable);
    this.errorCode=String.valueOf(Errors.SYSTEM_ERROR.getErrorCode());
    this.errorDescription = throwable.getMessage();
  }
  
  
  public KafkaRuntimeException(Errors errors, String message) {
    super(message);
    this.errorCode = String.valueOf(errors.getErrorCode());
    this.errorDescription = String.join(DASH,errors.name(),message);
  }

}
