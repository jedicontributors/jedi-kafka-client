package jedi.kafka.model;

public enum StepDetails {
  READ_CONFIGURATION(0,"Reading Configuration "),
  PRE_VALIDATION(1, "Prevalidation"),
  CONSTRUCTION(2, "Construction"),
  GRACEFUL_SHUTDOWN(3,"Graceful Shutdonw Configuration"),
  FINALIZE(4,"Finalize");

  private int code;
  private String description;

  StepDetails(int code, String description) {
    this.code = code;
    this.description = description;
  }

  public int getCode() {
    return code;
  }

  public String getDescription() {
    return description;
  }
}
