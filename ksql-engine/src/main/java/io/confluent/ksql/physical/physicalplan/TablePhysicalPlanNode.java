package io.confluent.ksql.physical.physicalplan;

import io.confluent.ksql.physical.exec.ExecutionPlan;

/**
 * Created by hojjat on 10/26/17.
 */
public class TablePhysicalPlanNode extends  PhysicalPlanNode {

  @Override
  public ExecutionPlan buildExecutionPlan() {
    return null;
  }

  @Override
  public double getCost() {
    return 0;
  }

  @Override
  public <C, R> R accept(PhysicalPlanVisitor<C, R> visitor, C context) {
    return visitor.visitPlan(this, context);
  }
}
