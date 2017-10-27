package io.confluent.ksql.physical.physicalplan;

import io.confluent.ksql.physical.exec.ExecutionPlan;
import io.confluent.ksql.physical.exec.ExecutionPlanner;

/**
 * Created by hojjat on 10/26/17.
 */
public class OutputPhysicalPlanNode extends PhysicalPlanNode {

  @Override
  public ExecutionPlan buildExecutionPlan(ExecutionPlanner executionPlanner) {
    return null;
  }

  @Override
  public <C, R> R accept(PhysicalPlanVisitor<C, R> visitor, C context) {
    return visitor.visitOutputPhysicalPlanNode(this, context);
  }

  @Override
  public double getCost() {
    return 0;
  }
}
