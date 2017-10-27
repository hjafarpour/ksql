package io.confluent.ksql.physical;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import io.confluent.ksql.planner.plan.PlanVisitor;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.serde.DataSource;

/**
 * Created by hojjat on 10/27/17.
 */
public class SamplePhysicalPlanBuildingStrategy extends PlanVisitor<Object, List<PhysicalPlanNode>> {

  @Override
  public List<PhysicalPlanNode> visitFilter(FilterNode node, Object context) {
    List<PhysicalPlanNode> physicalPlanList = node.getSource().accept(this, context);
    List<PhysicalPlanNode> updatedPlanList = new ArrayList<>();
    for (PhysicalPlanNode physicalPlanNode: physicalPlanList) {
      FilterPhysicalPlanNode filterPhysicalPlanNode = new FilterPhysicalPlanNode();
      filterPhysicalPlanNode.getChildren().add(physicalPlanNode);
      updatedPlanList.add(filterPhysicalPlanNode);
    }
    return updatedPlanList;
  }

  @Override
  public List<PhysicalPlanNode> visitProject(ProjectNode node, Object context) {
    List<PhysicalPlanNode> physicalPlanList = node.getSource().accept(this, context);
    List<PhysicalPlanNode> updatedPlanList = new ArrayList<>();
    for (PhysicalPlanNode physicalPlanNode: physicalPlanList) {
      ProjectPhysicalPlanNode projectPhysicalPlanNode = new ProjectPhysicalPlanNode();
      projectPhysicalPlanNode.getChildren().add(physicalPlanNode);
      updatedPlanList.add(projectPhysicalPlanNode);
    }
    return updatedPlanList;
  }

  @Override
  public List<PhysicalPlanNode> visitStructuredDataSourceNode(StructuredDataSourceNode node, Object context) {
    if (node.getDataSourceType() == DataSource.DataSourceType.KSTREAM) {
      return Collections.singletonList(new StreamPhysicalPlanNode());
    } else {
      return Collections.singletonList(new TablePhysicalPlanNode());
    }
  }

  @Override
  public List<PhysicalPlanNode> visitAggregate(AggregateNode node, Object context) {
    List<PhysicalPlanNode> physicalPlanList = node.getSource().accept(this, context);
    List<PhysicalPlanNode> updatedPlanList = new ArrayList<>();
    for (PhysicalPlanNode physicalPlanNode: physicalPlanList) {
      AggregatePhysicalPlanNode aggregatePhysicalPlanNode = new AggregatePhysicalPlanNode();
      aggregatePhysicalPlanNode.getChildren().add(physicalPlanNode);
      updatedPlanList.add(aggregatePhysicalPlanNode);
    }
    return updatedPlanList;
  }

  @Override
  public List<PhysicalPlanNode> visitJoin(JoinNode node, Object context) {
    List<PhysicalPlanNode> leftPhysicalPlanList = node.getLeft().accept(this, context);
    List<PhysicalPlanNode> rightPhysicalPlanList = node.getLeft().accept(this, context);
    List<PhysicalPlanNode> updatedPlanList = new ArrayList<>();
    for (PhysicalPlanNode leftPhysicalPlanNode: leftPhysicalPlanList) {
      for (PhysicalPlanNode rightPhysicalPlanNode: rightPhysicalPlanList) {
        HashJoinPhysicalPlanNode hashJoinPhysicalPlanNode = new HashJoinPhysicalPlanNode();
        hashJoinPhysicalPlanNode.getChildren().add(leftPhysicalPlanNode);
        hashJoinPhysicalPlanNode.getChildren().add(rightPhysicalPlanNode);

        BroadcastJoinPhysicalPlanNode broadcastJoinPhysicalPlanNode = new
            BroadcastJoinPhysicalPlanNode();
        broadcastJoinPhysicalPlanNode.getChildren().add(leftPhysicalPlanNode);
        broadcastJoinPhysicalPlanNode.getChildren().add(rightPhysicalPlanNode);
        updatedPlanList.add(hashJoinPhysicalPlanNode);
        updatedPlanList.add(broadcastJoinPhysicalPlanNode);
      }
    }
    return updatedPlanList;
  }

  @Override
  public List<PhysicalPlanNode> visitOutput(OutputNode node, Object context) {
    List<PhysicalPlanNode> physicalPlanList = node.getSource().accept(this, context);
    List<PhysicalPlanNode> updatedPlanList = new ArrayList<>();
    for (PhysicalPlanNode physicalPlanNode: physicalPlanList) {
      OutputPhysicalPlanNode outputPhysicalPlanNode = new OutputPhysicalPlanNode();
      outputPhysicalPlanNode.getChildren().add(physicalPlanNode);
      updatedPlanList.add(outputPhysicalPlanNode);
    }
    return updatedPlanList;
  }

}