package io.confluent.ksql.physical.exec;

import org.apache.kafka.common.serialization.Serde;

import javax.xml.validation.Schema;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.parser.tree.Extract;
import io.confluent.ksql.serde.KsqlTopicSerDe;

/**
 * Created by hojjat on 10/26/17.
 */
public interface Stream extends ExecutionPlan {

  GroupedStream groupByKey(Serde<String> string, Serde<GenericRow> genericRowSerde);
  Stream selectKey(Extract.Field field);
  Stream leftJoin(Table table, Schema schema, Extract.Field field, KsqlTopicSerDe joinSerDe);

}
