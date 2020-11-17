package jedi.kafka.model;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class AnySerializableObject implements Serializable {

  private static final long serialVersionUID = 6846020384683239029L;

  private String message;
  
}
