package io.confluent.ksql.physical.cost;

import io.confluent.ksql.physical.physicalplan.AggregatePhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.BroadcastJoinPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.FilterPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.HashJoinPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.OutputPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.PhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.PhysicalPlanVisitor;
import io.confluent.ksql.physical.physicalplan.ProjectPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.StreamPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.TablePhysicalPlanNode;

/**
 * Created by hojjat on 10/27/17.
 */
public class CostEstimator<C, R> extends PhysicalPlanVisitor<C, R> {

  protected R visitPhysicalPlanNode(PhysicalPlanNode node, C context) {
    return null;
  }

  public R process(PhysicalPlanNode node, C context) {
    return node.accept(this, context);
  }

  public R visitAggregatePhysicalPlanNode(AggregatePhysicalPlanNode node, C context) {
    return visitPhysicalPlanNode(node, context);
  }

  public R visitBroadcastJoinPhysicalPlanNode(BroadcastJoinPhysicalPlanNode node, C context) {
    return visitPhysicalPlanNode(node, context);
  }

  public R visitHashJoinPhysicalPlanNode(HashJoinPhysicalPlanNode node, C context) {
    return visitPhysicalPlanNode(node, context);
  }

  public R visitOutputPhysicalPlanNode(OutputPhysicalPlanNode node, C context) {
    return visitPhysicalPlanNode(node, context);
  }

  public R visitFilterPhysicalPlanNode(FilterPhysicalPlanNode node, C context) {
    return visitPhysicalPlanNode(node, context);
  }

  public R visitProjectPhysicalPlanNode(ProjectPhysicalPlanNode node, C context) {
    return visitPhysicalPlanNode(node, context);
  }

  public R visitStreamPhysicalPlanNode(StreamPhysicalPlanNode node, C context) {
    return visitPhysicalPlanNode(node, context);
  }

  public R visitTablePhysicalPlanNode(TablePhysicalPlanNode node, C context) {
    return visitPhysicalPlanNode(node, context);
  }


}
