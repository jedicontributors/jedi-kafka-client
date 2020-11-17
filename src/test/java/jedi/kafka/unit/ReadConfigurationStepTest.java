package jedi.kafka.unit;

import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import jedi.kafka.service.ReadConfigurationStep;

@RunWith(MockitoJUnitRunner.class)
public class ReadConfigurationStepTest extends StepTest<ReadConfigurationStep>{
  
  private ReadConfigurationStep readConfigurationStep;
  
  public ReadConfigurationStepTest() {
    super(ReadConfigurationStep.class);
    readConfigurationStep = getStep();
  }
  
  @Test
  public void test_non_null_kafka_service_config() throws Exception{
    readConfigurationStep.process();
    assertNotNull(kafkaService.getKafkaServiceConfig());
  }
  
}
