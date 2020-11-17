package jedi.kafka.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Response {
  private static final String SUCCESS_MESSAGE = "SUCCESS";
  private int responseCode;
  private String responseDescription;
  public static final int SUCCESS = 0;
  public static final int FAILED = -1;
  private static Response successResponse = new Response(SUCCESS, SUCCESS_MESSAGE);
  
  public boolean isSuccess() {
    return SUCCESS == responseCode;
  }
  
  public static Response getSuccessResult() {
    return successResponse;
  }
}
