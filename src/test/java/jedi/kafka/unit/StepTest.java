package jedi.kafka.unit;

import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import jedi.kafka.model.Errors;
import jedi.kafka.model.KafkaRuntimeException;
import jedi.kafka.model.KafkaServiceConfig;
import jedi.kafka.model.MockedKafkaService;
import jedi.kafka.model.Step;
import jedi.kafka.service.KafkaService;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public abstract class StepTest<T extends Step> {

  protected KafkaService kafkaService;
  
  protected KafkaServiceConfig kafkaServiceConfig;  
  
  @Getter
  protected T step;
  
  public StepTest(Class<T> t) {
    try {
      kafkaService = new MockedKafkaService();
      kafkaServiceConfig = Mockito.spy(KafkaServiceConfig.class);
      kafkaService.setKafkaServiceConfig(kafkaServiceConfig);
      step = Mockito.spy(t.getDeclaredConstructor(KafkaService.class,Step.class).newInstance(kafkaService,Mockito.mock(Step.class)));
    } catch (Exception e) {
      log.error("Exception in spying object {}",t.getClass());
      throw new KafkaRuntimeException(Errors.INVALID_REQUEST_ERROR,e.toString());
    }
  }
}
