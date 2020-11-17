package jedi.kafka.model;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedThreadFactory implements ThreadFactory {

  private AtomicInteger threadCounter = new AtomicInteger(0);
  private String poolName;
  private boolean daemon;

  public NamedThreadFactory(String poolName, boolean daemon) {
    this.poolName = poolName;
    this.daemon = daemon;
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread thread = new Thread(runnable, this.poolName + "-" + threadCounter.incrementAndGet());
    thread.setDaemon(this.daemon);
    return thread;
  }
}