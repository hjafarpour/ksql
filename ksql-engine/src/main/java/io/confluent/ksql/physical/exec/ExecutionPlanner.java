package io.confluent.ksql.physical.exec;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.Topology;

/**
 * Created by hojjat on 10/26/17.
 */
public interface ExecutionPlanner {

  Stream stream(String kafkaTopicName, Field windowedGenericRowConsumed);

  Table windowedTable(String kafkaTopicName, Topology.AutoOffsetReset autoOffsetReset);

  Table table(String kafkaTopicName, Topology.AutoOffsetReset autoOffsetReset);
}
