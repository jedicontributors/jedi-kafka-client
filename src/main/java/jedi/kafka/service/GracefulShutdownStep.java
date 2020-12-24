package jedi.kafka.service;

import static jedi.kafka.model.KafkaConstants.KAFKA_SERVICE;
import static jedi.kafka.model.KafkaConstants.DEFAULT_MAX_PROCESS_TIME;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jedi.kafka.model.Step;
import jedi.kafka.model.StepDetails;
import jedi.kafka.util.ThreadPoolUtil;
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
        ExecutorService es = ThreadPoolUtil.newCachedLimitedThreadPool("GraceFulShutdown");
        log.info("Closing kafkaService components now..");
        List<Future<?>> futures = new ArrayList<>();
        CountDownLatch countDownLatch  = new CountDownLatch(kafkaService.getTopicConsumerThreadMap().size());
        log.info("Waiting max 30 seconds for consumer tasks to finish");
        long startTime = System.currentTimeMillis();
        for(ConsumerThread ct:kafkaService.getTopicConsumerThreadMap().values()) {
          futures.add(es.submit(()->ct.shutdown(countDownLatch)));
        }
        try {
          countDownLatch.await(DEFAULT_MAX_PROCESS_TIME, TimeUnit.SECONDS);
          for(Future<?> fut:futures) {
            fut.get(DEFAULT_MAX_PROCESS_TIME,TimeUnit.MILLISECONDS);
          }
          long remainingTime = DEFAULT_MAX_PROCESS_TIME-(System.currentTimeMillis()-startTime);
          if(remainingTime<3000) {
            remainingTime = 3000;
          }
          kafkaService.close(remainingTime);
          log.info("Producer(s) closed");
        } catch (InterruptedException e) {
          log.error("Interrupt on waiting countdown ",e);
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          log.error("",e);
        }
        log.info("Jedi-Kafka-Client Shutdown finished!");
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
          log.error("Pool did not terminate waiting another 15 seconds to terminate");
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
