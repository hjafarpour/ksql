package io.confluent.ksql.physical.exec;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.Topology;

/**
 * This class and the other classes in this package work the same as Damian's suggestion.
 *
 * Created by hojjat on 10/26/17.
 */
public interface ExecutionPlanner {

  Stream stream(String kafkaTopicName, Field windowedGenericRowConsumed);

  Table windowedTable(String kafkaTopicName, Topology.AutoOffsetReset autoOffsetReset);

  Table table(String kafkaTopicName, Topology.AutoOffsetReset autoOffsetReset);
}
