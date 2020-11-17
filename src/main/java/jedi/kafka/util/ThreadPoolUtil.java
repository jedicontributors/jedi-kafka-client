package jedi.kafka.util;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import jedi.kafka.model.NamedThreadFactory;
import lombok.experimental.UtilityClass;

@UtilityClass
public final class ThreadPoolUtil {

  private static final int INITIAL_CORE_POOL_SIZE = 0;

  private static final int MAX_IDLE_SECONDS = 60;
  
  public static ThreadPoolExecutor newCachedLimitedThreadPool(String threadNamePrefix) {
    return newCachedLimitedThreadPool(threadNamePrefix,null);
  }

  public static ThreadPoolExecutor newCachedLimitedThreadPool(String threadNamePrefix,Integer maxThreads) {
    Integer maxThreadsLocal = maxThreads;
    if (Objects.isNull(maxThreadsLocal)) {
      maxThreadsLocal = Integer.valueOf(Runtime.getRuntime().availableProcessors() * 2);
    }
    return new ThreadPoolExecutor(INITIAL_CORE_POOL_SIZE, maxThreadsLocal, MAX_IDLE_SECONDS,
        TimeUnit.SECONDS, new ArrayBlockingQueue<>(2 * maxThreadsLocal),
        new NamedThreadFactory(threadNamePrefix, false), new ThreadPoolExecutor.CallerRunsPolicy());
  }
}
