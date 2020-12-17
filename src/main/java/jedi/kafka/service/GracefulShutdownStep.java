package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.KAFKA_SERVICE;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import jedi.kafka.model.Step;
import jedi.kafka.model.StepDetails;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GracefulShutdownStep extends Step {
  
  public GracefulShutdownStep(KafkaService kafkaService, Step next) {
    super(kafkaService, next);
  }

  @Override
  public StepDetails getServiceStep() {
    return StepDetails.GRACEFUL_SHUTDOWN;
  }

  @Override
  public void process() {
    Runtime.getRuntime().addShutdownHook(new Thread(KAFKA_SERVICE) {
      @Override
      public void run() {
        log.info("Closing kafkaService components now..");
        shutdownAndAwaitTermination(kafkaService.producerThreadPool);
        log.info("Producer ExecutorService shutdown finished");
        kafkaService.getTopicConsumerThreadMap().values().forEach(ConsumerThread::shutdown);
        try {
          log.info("Waiting max 30 seconds for consumer tasks to finish");
          Thread.sleep(15000L);
        } catch (InterruptedException e) {
          log.error("InterruptedException on sleeping",e);
          Thread.currentThread().interrupt();
        }finally {
          log.info("Shutdown finished!");
        }
      }
    });
    
  }
  
  public static void shutdownAndAwaitTermination(ExecutorService pool) {
    log.info("Shutting down pool -> {}",pool.toString());
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(15, TimeUnit.SECONDS)) {
        log.info("Waiting pool to shutdown");
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(15, TimeUnit.SECONDS))
          log.error("Pool did not terminate");
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      log.error("InterruptedException in Pool termination.trying to shut down again");
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

}
