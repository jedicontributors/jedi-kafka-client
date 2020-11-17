package jedi.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum Errors {
  
	SYSTEM_ERROR(-1), 
	INVALID_REQUEST_ERROR(-2),
	INVALID_CONFIGURATION_ERROR(-3),
	INVALID_SERVICE_INPUT_ERROR(-4),
	INVALID_SERVICE_OUTPUT_ERROR(-5),
	UNSUPPORTED_OPERATION(-6),
    TIMEOUT_ERROR(-7);
  
	private int errorCode;

}
