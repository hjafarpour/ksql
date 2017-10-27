package io.confluent.ksql.physical.physicalplan;

import java.util.ArrayList;
import java.util.List;

import io.confluent.ksql.physical.exec.ExecutionPlan;
import io.confluent.ksql.physical.exec.ExecutionPlanner;

/**
 * Created by hojjat on 10/26/17.
 */
public abstract class PhysicalPlanNode {

  PhysicalPlanNode parePhysicalPlanNode;

  List<PhysicalPlanNode> children = new ArrayList<>();

  /**
   * Generates the execution plan based on the destination system.
   *
   * @param executionPlanner
   * @return
   */
  public abstract ExecutionPlan buildExecutionPlan(ExecutionPlanner executionPlanner);

  public <C, R> R accept(PhysicalPlanVisitor<C, R> visitor, C context) {
    return visitor.visitPhysicalPlanNode(this, context);
  }

  public abstract double getCost();

  public List<PhysicalPlanNode> getChildren() {
    return children;
  }

  public void setChildren(
      List<PhysicalPlanNode> children) {
    this.children = children;
  }

  public PhysicalPlanNode getParePhysicalPlanNode() {
    return parePhysicalPlanNode;
  }

  public void setParePhysicalPlanNode(
      PhysicalPlanNode parePhysicalPlanNode) {
    this.parePhysicalPlanNode = parePhysicalPlanNode;
  }
}
