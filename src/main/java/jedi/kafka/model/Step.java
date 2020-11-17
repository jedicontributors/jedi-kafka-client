package jedi.kafka.model;

import java.util.Objects;

import jedi.kafka.service.KafkaService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Step {

  private Step next;
  
  protected KafkaService kafkaService;
  
  public Step(KafkaService kafkaService,Step next) {
    this.kafkaService = kafkaService;
    this.next = next;
  }

  public abstract StepDetails getServiceStep();
  
  public abstract void process();
  
  public void execute() {
    log.info("Starting {} step",getServiceStep().getDescription());
    this.process();
    log.info("Finished {} step",getServiceStep().getDescription());
    if (Objects.nonNull(next)) {
      next.execute();
    }
  }
  
}
