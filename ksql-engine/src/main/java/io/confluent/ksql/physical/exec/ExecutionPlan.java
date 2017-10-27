package io.confluent.ksql.physical.exec;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.KeyValueMapper;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.util.Pair;

/**
 * Created by hojjat on 10/26/17.
 */
public interface ExecutionPlan {
  Schema getSchema();

  ExecutionPlan withSchema(Schema schema);

  ExecutionPlan filter(Expression havingExpressions);

  ExecutionPlan selectKey(KeyValueMapper<String, GenericRow, String> mapper);

  ExecutionPlan select(List<Pair<String,Expression>> finalSelectExpressions);

  ExecutionPlan groupByKey(Serde<String> string, Serde<GenericRow> genericRowSerde);

  ExecutionPlan keyField();

  ExecutionPlan withOutputNode(OutputNode ksqlBareOutputNode);

  ExecutionPlan withLimit(Optional<Integer> limit);

  ExecutionPlan into(String kafkaTopicName, Serde<GenericRow> rowSerDe, Set<Integer> rowkeyIndexes);
}
