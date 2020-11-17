package jedi.kafka.unit;

import static jedi.kafka.model.KafkaConstants.DASH;
import static jedi.kafka.model.KafkaConstants.DEFAULT_MAX_POLL_INTERVAL_MS;
import static jedi.kafka.model.KafkaConstants.MAX_POLL_INTERVAL_MS;
import static jedi.kafka.model.TestConstants.TEST_KAFKA_CONFIG_FILENAME;
import static jedi.kafka.model.TestConstants.TEST_TOPIC;
import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import jedi.kafka.model.KafkaConstants;
import jedi.kafka.model.KafkaServiceConfig.KafkaConsumerConfig;
import jedi.kafka.model.Response;
import jedi.kafka.model.StringConsumer;
import jedi.kafka.service.ConsumerHandler;
import jedi.kafka.service.ConsumerThread;
import jedi.kafka.service.KafkaService;
import jedi.kafka.util.ThreadPoolUtil;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RunWith(MockitoJUnitRunner.class)
public class ConsumerThreadTest {
  
  
  private static final long SHORT_SLEEP = 1000L;
  
  private static final long LONG_SLEEP = 10000L;
  
  private Consumer<?,?> consumer;
  
  @SuppressWarnings("rawtypes")
  private MockConsumer mockConsumer;
  
  private static final int partition = 0;
  
  private KafkaService kafkaService = new KafkaService(TEST_KAFKA_CONFIG_FILENAME);

  @Mock
  private Response response = new Response();
  
  public class TestConsumerWithDelay implements ConsumerHandler<String> {
    
    @Getter
    private AtomicLong counter = new AtomicLong();
    
    @Override
    public Response onMessage(String message) {
      log.debug(counter.incrementAndGet()+"-Recieved message "+message);
      try {
        Thread.currentThread().join(LONG_SLEEP);
      } catch (InterruptedException e) {
        log.error("Error::",e);
      }
      return response;
    }
  }
  
  private StringConsumer testConsumer = Mockito.spy(new StringConsumer());
  
  private ConsumerThread consumerThread;
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Before
  public void init() throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
    testConsumer.setResponse(response);
    consumer = new MockConsumer(OffsetResetStrategy.EARLIEST);
    mockConsumer = (MockConsumer)consumer;
    Map<TopicPartition, Long> startOffsets = new HashMap<>();
    TopicPartition tp = new TopicPartition(TEST_TOPIC, partition);
    startOffsets.put(tp, 0L);
    mockConsumer.updateBeginningOffsets(startOffsets);
    mockConsumer.schedulePollTask(() -> {
      mockConsumer.rebalance(Collections.singletonList(new TopicPartition(TEST_TOPIC, partition)));
    });
  }
  
  @Test
  public void test_recieves_success_messages() throws InterruptedException {
    int size = 10;
    consumerThread = createConsumerThreadByConsumerName(TEST_TOPIC,consumer,testConsumer);
    consumerThread.start();
    sendRecords(size);
    consumerThread.join(SHORT_SLEEP);
    Mockito.verify(testConsumer,Mockito.times(size)).onMessage(Mockito.any());
    assertEquals(size,testConsumer.getCounter().longValue());
  }

  @Test
  public void test_long_processing_handler() throws InterruptedException {
    int size = 1;
    sendRecords(size);
    TestConsumerWithDelay testConsumerWithDelay = Mockito.spy(new TestConsumerWithDelay());
    ConsumerThread consumerThread = Mockito.spy(createConsumerThreadByConsumerName(TEST_TOPIC,consumer,testConsumerWithDelay));
    consumerThread.getConsumerConfig().getProperties().put(KafkaConstants.MAX_POLL_INTERVAL_MS, LONG_SLEEP);
    consumerThread.start();
    consumerThread.join(SHORT_SLEEP);
    Mockito.verify(testConsumerWithDelay,Mockito.times(size)).onMessage(Mockito.any());
    assertEquals(size,testConsumerWithDelay.getCounter().longValue());
  }
  
  @SuppressWarnings("unchecked")
  private void sendRecords(long size) {
    mockConsumer.schedulePollTask(() -> {
      createRecords(size).stream().forEach(msg -> 
      mockConsumer.addRecord(msg));
    });
  }
  
  private List<ConsumerRecord<String,byte[]>> createRecords(long size) {
    return LongStream.range(0, size).mapToObj(i -> {
      return new ConsumerRecord<String,byte[]>(TEST_TOPIC, partition, i,String.valueOf(UUID.randomUUID()),SerializationUtils.serialize("Message"+i));
    }).collect(Collectors.toList());
  }

  private ConsumerThread createConsumerThreadByConsumerName(String consumerTopic, Consumer<?,?> mockConsumer,ConsumerHandler<?> handler) {
    KafkaConsumerConfig<?,?> consumerConfig = kafkaService.getTopicKafkaConsumerConfigMap().get(consumerTopic);
    long pollTime = Long.parseLong((String) consumerConfig.getProperties().getOrDefault(MAX_POLL_INTERVAL_MS, DEFAULT_MAX_POLL_INTERVAL_MS));
    Duration pollDuration = Duration.ofMillis(pollTime);
    String threadName = String.join(DASH,consumerConfig.getTopic(),String.valueOf(0));
    boolean retyable = Objects.nonNull(consumerConfig.getRetryPolicys())&& !consumerConfig.getRetryPolicys().isEmpty();
    ConsumerThread consumerThread = new ConsumerThread();
    consumerThread.setKafkaService(kafkaService);
    consumerThread.setConsumerConfig(consumerConfig);
    consumerThread.setDuration(pollDuration);
    consumerThread.setHandler(handler);
    consumerThread.setName(threadName);
    consumerThread.setRetryable(retyable);
    consumerThread.setKafkaConsumer(mockConsumer);
    consumerThread.setExecutorService(ThreadPoolUtil.newCachedLimitedThreadPool(threadName,consumerConfig.getMaximumThreadCount()));
    consumerThread.setMaxProcessTime(5000L);
    return consumerThread;
  }
  
}
