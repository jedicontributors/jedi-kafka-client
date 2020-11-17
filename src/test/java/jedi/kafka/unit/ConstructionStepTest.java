package jedi.kafka.unit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import jedi.kafka.service.ConstructionStep;

@RunWith(MockitoJUnitRunner.class)
public class ConstructionStepTest extends StepTest<ConstructionStep>{
  
  private ConstructionStep constructionStep;
  
  
  public ConstructionStepTest() {
    super(ConstructionStep.class);
    constructionStep = getStep();
  }
  
  @Test
  public void test_already_defined_retry_consumer() {
    constructionStep.process();
  }

  
}
