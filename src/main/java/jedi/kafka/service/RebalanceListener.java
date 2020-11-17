package jedi.kafka.service;

import java.util.Collection;

import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

public class RebalanceListener extends NoOpConsumerRebalanceListener {

  private ConsumerThread consumerThread;

  
  
  public RebalanceListener(ConsumerThread consumerThread) {
      this.consumerThread = consumerThread;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      // save the offsets in an external store using some custom code not described here
      for(TopicPartition partition: partitions)
        consumerThread.getAssignedPartitions().remove(partition);
  }

  public void onPartitionsLost(Collection<TopicPartition> partitions) {
      // do not need to save the offsets since these partitions are probably owned by other consumers already
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
      // read the offsets from an external store using some custom code not described here
      consumerThread.getKafkaConsumer().seekToEnd(partitions);
      consumerThread.setAssignedPartitions(partitions);
  }
  
}
