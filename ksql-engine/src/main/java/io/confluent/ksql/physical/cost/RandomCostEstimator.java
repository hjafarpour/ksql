package io.confluent.ksql.physical.cost;

import io.confluent.ksql.physical.physicalplan.AggregatePhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.BroadcastJoinPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.FilterPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.HashJoinPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.OutputPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.ProjectPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.StreamPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.TablePhysicalPlanNode;

/**
 * Created by hojjat on 10/27/17.
 */
public class RandomCostEstimator extends CostEstimator<Double, Double> {

  private double getRandomCost() {
    return (int)(10 * Math.random());
  }

  @Override
  public Double visitStreamPhysicalPlanNode(StreamPhysicalPlanNode
                                                          streamPhysicalPlanNode, Double cost) {
    return getRandomCost();
  }

  @Override
  public Double visitTablePhysicalPlanNode(TablePhysicalPlanNode
                                                tablePhysicalPlanNode, Double cost) {
    return getRandomCost();
  }

  @Override
  public Double visitBroadcastJoinPhysicalPlanNode(BroadcastJoinPhysicalPlanNode node, Double cost) {
    Double leftCost = node.getChildren().get(0).accept(this, cost);
    Double rightCost = node.getChildren().get(1).accept(this, cost);
    return getRandomCost() + leftCost + rightCost;
  }

  @Override
  public Double visitHashJoinPhysicalPlanNode(HashJoinPhysicalPlanNode node, Double cost) {
    Double leftCost = node.getChildren().get(0).accept(this, cost);
    Double rightCost = node.getChildren().get(1).accept(this, cost);
    return getRandomCost() + leftCost + rightCost;
  }

  @Override
  public Double visitOutputPhysicalPlanNode(OutputPhysicalPlanNode node, Double cost) {
    return getRandomCost() + node.getChildren().get(0).accept(this, cost);
  }

  @Override
  public Double visitProjectPhysicalPlanNode(ProjectPhysicalPlanNode node, Double cost) {
    return getRandomCost() + node.getChildren().get(0).accept(this, cost);
  }

  @Override
  public Double visitFilterPhysicalPlanNode(FilterPhysicalPlanNode node, Double cost) {
    return getRandomCost() + node.getChildren().get(0).accept(this, cost);
  }

  @Override
  public Double visitAggregatePhysicalPlanNode(AggregatePhysicalPlanNode node, Double cost) {
    return getRandomCost() + node.getChildren().get(0).accept(this, cost);
  }
}
