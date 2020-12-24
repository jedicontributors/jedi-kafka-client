package jedi.kafka.util;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import jedi.kafka.model.KafkaConstants;
import jedi.kafka.model.NamedThreadFactory;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class ThreadPoolUtil {

  private static final int MAX_IDLE_SECONDS = 60;
  
  public static ThreadPoolExecutor newCachedLimitedThreadPool(String threadNamePrefix) {
    return newCachedLimitedThreadPool(threadNamePrefix,null);
  }

  public static ThreadPoolExecutor newCachedLimitedThreadPool(String threadNamePrefix,Integer maxThreads) {
    Integer maxThreadsLocal = maxThreads;
    if (Objects.isNull(maxThreadsLocal)) {
      maxThreadsLocal = Integer.valueOf(Runtime.getRuntime().availableProcessors());
      if(maxThreadsLocal<=1) {
        maxThreadsLocal = KafkaConstants.DEFAULT_MAX_THREADS;
      }
    }
    return new ThreadPoolExecutor(maxThreadsLocal*2, maxThreadsLocal*2, MAX_IDLE_SECONDS,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(maxThreadsLocal*2),
        new NamedThreadFactory(threadNamePrefix, false), new ThreadPoolExecutor.CallerRunsPolicy());
  }
  
  public static ThreadPoolExecutor newCachedLimitedThreadPool(String threadNamePrefix,Integer maxThreads,Integer queueSize) {
    return new ThreadPoolExecutor(maxThreads, maxThreads, MAX_IDLE_SECONDS,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(queueSize),
        new NamedThreadFactory(threadNamePrefix, false), new ThreadPoolExecutor.CallerRunsPolicy());
  }
}
