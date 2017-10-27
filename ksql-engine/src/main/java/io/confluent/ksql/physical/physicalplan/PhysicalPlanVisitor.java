package io.confluent.ksql.physical.physicalplan;

/**
 * Created by hojjat on 10/26/17.
 */
public class PhysicalPlanVisitor<C, R> {
  protected R visitPlan(PhysicalPlanNode node, C context) {
    return null;
  }
}
