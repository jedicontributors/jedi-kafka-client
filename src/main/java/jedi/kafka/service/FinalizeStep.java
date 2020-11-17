package jedi.kafka.service;

import jedi.kafka.model.StepDetails;
import lombok.extern.slf4j.Slf4j;

import org.apache.kafka.clients.producer.KafkaProducer;

import jedi.kafka.model.Step;
@Slf4j
public class FinalizeStep extends Step{

  public FinalizeStep(KafkaService kafkaService, Step next) {
    super(kafkaService, next);
  }

  @Override
  public StepDetails getServiceStep() {
    return StepDetails.FINALIZE;
  }

  @Override
  public void process() {
    kafkaService.topicKafkaProducerConfigMap.values().stream().forEach(kpp->{
      log.info("Creating Producer for topic {}",kpp.getTopic());
      kpp.setKafkaProducer(new KafkaProducer<>(kpp.getProperties())); 
    });
  }
}
