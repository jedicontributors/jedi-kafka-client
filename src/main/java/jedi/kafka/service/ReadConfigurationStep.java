package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.CONTEXT_SEPERATOR;
import static jedi.kafka.model.KafkaConstants.UTF_8;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Objects;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import jedi.kafka.model.StepDetails;
import lombok.extern.slf4j.Slf4j;
import jedi.kafka.model.Errors;
import jedi.kafka.model.KafkaRuntimeException;
import jedi.kafka.model.KafkaServiceConfig;
import jedi.kafka.model.Step;

@Slf4j
public class ReadConfigurationStep extends Step {

  private Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
  
  public ReadConfigurationStep(KafkaService kafkaService,Step serviceChain) {
    super(kafkaService,serviceChain);
  }

  @Override
  public StepDetails getServiceStep() {
    return StepDetails.READ_CONFIGURATION;
  }

  @Override
  public void process() {
    String fileName = kafkaService.getConfigurationFileName();
    if(Objects.isNull(fileName)) {
      log.error("Resource filename for kafka configuration can not be null");
      throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,"Resource filename for kafka configuration can not be null");
    }
    if(!fileName.startsWith(CONTEXT_SEPERATOR)) {
      fileName = CONTEXT_SEPERATOR+fileName;
    }
    InputStream inputStream = this.getClass().getResourceAsStream(fileName);
    try {
      if(Objects.isNull(inputStream)) {
        log.error("Can not read file {}",fileName);
        throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,"Can not read file "+fileName+" Make sure it is in classpath");
      }
      log.debug("Parsing json file  {}",fileName);
      JsonElement jsonObject = JsonParser.parseReader(new InputStreamReader(inputStream, UTF_8));
      log.info("Successfully read kafka properties {}",fileName);
      kafkaService.kafkaServiceConfig = gson.fromJson(jsonObject.toString(),KafkaServiceConfig.class);
    } catch (Exception e) {
      log.error("Error occured reading kafka properties file {}",fileName,e);
      throw new KafkaRuntimeException(Errors.INVALID_CONFIGURATION_ERROR,"Error occured when generate kafka properties from "+ fileName + " erros is " + e.getMessage());
    }
    
  }
}
