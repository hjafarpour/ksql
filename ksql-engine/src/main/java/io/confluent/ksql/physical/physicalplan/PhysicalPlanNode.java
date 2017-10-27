package io.confluent.ksql.physical.physicalplan;

import java.util.ArrayList;
import java.util.List;

import io.confluent.ksql.physical.exec.ExecutionPlan;

/**
 * Created by hojjat on 10/26/17.
 */
public abstract class PhysicalPlanNode {

  List<PhysicalPlanNode> children = new ArrayList<>();
  public abstract ExecutionPlan buildExecutionPlan();

  public <C, R> R accept(PhysicalPlanVisitor<C, R> visitor, C context) {
    return visitor.visitPlan(this, context);
  }

  public abstract double getCost();

  public List<PhysicalPlanNode> getChildren() {
    return children;
  }

  public void setChildren(
      List<PhysicalPlanNode> children) {
    this.children = children;
  }
}
