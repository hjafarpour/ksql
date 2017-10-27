package io.confluent.ksql.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.confluent.ksql.physical.cost.RandomCostEstimator;
import io.confluent.ksql.physical.physicalplan.AggregatePhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.BroadcastJoinPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.FilterPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.HashJoinPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.OutputPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.PhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.ProjectPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.StreamPhysicalPlanNode;
import io.confluent.ksql.physical.physicalplan.TablePhysicalPlanNode;
import io.confluent.ksql.planner.plan.AggregateNode;
import io.confluent.ksql.planner.plan.FilterNode;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.OutputNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.PlanVisitor;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;

/**
 * This class will get an optimized logical plan and take the following steps:
 *
 * 1. From the given logical plan build a set of physical plans using the provided physical plan
 * generation strategy. The physical plan generation strategy is pluggable and we should be able
 * to provide different strategies or mix of several strategies with a desired order to build
 * variety of the physical plans
 *
 * 2. The initiall generated physical plans can further expanded to even bigger set of physical
 * plans using again pluggable strategies. One example is join ordering strategy that would build
 * physical plans by reordering joins.
 *
 * 3. Estimate the cost of each plan using the cost estimator tool. Again the cost estimator is
 * pluggable and we can provide different estimators easily. Note that we can embed the cost
 * estimation in the plan generation so we can guide it and prevent generation of high cost
 * oplans. Here, for simpilicity we have the cost estimation as a separate step.
 *
 * 4. Pick the physical plan with the lowest cost
 *
 * 5. Use the selected physical plan to build the desired execution plan, currently we have Kafka
 * Streams as the execution destination.
 *
 *
 * Created by hojjat on 10/26/17.
 */
public class NewPhysicalPlanBuilder {

  public Pair<PhysicalPlanNode, Double> getPhysicalPlan(PlanNode logicalPlan, PlanVisitor<Object,
      List<PhysicalPlanNode>> strategy) {

    /**
     * Build the first set of physical plans from the optimized logical plan.
     */
    List<PhysicalPlanNode> generatedPhysicalPland =  strategy.process(logicalPlan, null);

    /**
     * Extra optimizations can easily plugged in:
     */
//    List<PhysicalPlanNode> generatedPhysicalPlandAfterJoinReordering = joinReorderingOptimizer
//        .getAllPossiblePlans(generatedPhysicalPland);
//
//    Optimizer A
//    Optimizer B
//    ...

    /**
     * Cost estimation can be embedded in the plan generator.
     * For simpllicity here we first crreate plans and then estimate the costs.
     */
    PhysicalPlanNode selectedPhysicalPlan = null;
    Double selectedPhysicalPlanCost = Double.MAX_VALUE;

    for (PhysicalPlanNode physicalPlanNode: generatedPhysicalPland) {
      Double planCost = new RandomCostEstimator().process(physicalPlanNode, 0d);
      if (planCost <  selectedPhysicalPlanCost) {
        selectedPhysicalPlan = physicalPlanNode;
        selectedPhysicalPlanCost = planCost;
      }
    }

    if (selectedPhysicalPlan != null) {
      return new Pair<>(selectedPhysicalPlan, selectedPhysicalPlanCost);
    } else {
      throw new KsqlException("No physical plan was generated.");
    }
  }

}
