package jedi.kafka.model;

import com.google.gson.annotations.Expose;

import lombok.Data;

@Data
public class RetryPolicy{
  @Expose
  private String retryTopic;
  @Expose
  private String errorCode;
  @Expose
  private Long seconds;
  
}
