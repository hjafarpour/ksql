package io.confluent.ksql.physical.physicalplan;

import io.confluent.ksql.physical.exec.ExecutionPlan;
import io.confluent.ksql.physical.exec.ExecutionPlanner;

/**
 * Created by hojjat on 10/27/17.
 */
public class AggregatePhysicalPlanNode extends PhysicalPlanNode {

  @Override
  public double getCost() {
    return 0;
  }

  @Override
  public ExecutionPlan buildExecutionPlan(ExecutionPlanner executionPlanner) {
    return null;
  }

  @Override
  public <C, R> R accept(PhysicalPlanVisitor<C, R> visitor, C context) {
    return visitor.visitPhysicalPlanNode(this, context);
  }
}
