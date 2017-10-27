package io.confluent.ksql.physical;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.streams.Topology;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import io.confluent.ksql.analyzer.AggregateAnalysis;
import io.confluent.ksql.analyzer.AggregateAnalyzer;
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.analyzer.AnalysisContext;
import io.confluent.ksql.analyzer.Analyzer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.KsqlParser;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.physical.exec.ExecutionPlan;
import io.confluent.ksql.physical.exec.ExecutionPlanner;
import io.confluent.ksql.physical.exec.Stream;
import io.confluent.ksql.physical.exec.Table;
import io.confluent.ksql.physical.physicalplan.PhysicalPlanNode;
import io.confluent.ksql.planner.LogicalPlanner;
import io.confluent.ksql.planner.plan.JoinNode;
import io.confluent.ksql.planner.plan.PlanNode;
import io.confluent.ksql.planner.plan.ProjectNode;
import io.confluent.ksql.planner.plan.StructuredDataSourceNode;
import io.confluent.ksql.util.MetaStoreFixture;
import io.confluent.ksql.util.Pair;

/**
 * Created by hojjat on 10/27/17.
 */
public class NewPhysicalPlanBuilderTest {

  private static final KsqlParser KSQL_PARSER = new KsqlParser();

  private MetaStore metaStore;
  private FunctionRegistry functionRegistry;

  @Before
  public void init() {
    metaStore = MetaStoreFixture.getNewMetaStore();
    functionRegistry = new FunctionRegistry();
  }

  private PlanNode buildLogicalPlan(String queryStr) {
    List<Statement> statements = KSQL_PARSER.buildAst(queryStr, metaStore);
    // Analyze the query to resolve the references and extract oeprations
    Analysis analysis = new Analysis();
    Analyzer analyzer = new Analyzer(analysis, metaStore);
    analyzer.process(statements.get(0), new AnalysisContext(null));
    AggregateAnalysis aggregateAnalysis = new AggregateAnalysis();
    AggregateAnalyzer aggregateAnalyzer = new AggregateAnalyzer(aggregateAnalysis, analysis,
                                                                functionRegistry);
    for (Expression expression: analysis.getSelectExpressions()) {
      aggregateAnalyzer.process(expression, new AnalysisContext(null));
    }
    // Build a logical plan
    PlanNode logicalPlan = new LogicalPlanner(analysis, aggregateAnalysis, functionRegistry).buildPlan();
    return logicalPlan;
  }

  /**
   * Create a Kafka Streams execution planner.
   * @return
   */
  private ExecutionPlanner getKafkaStreamsExecutionPlanner() {
    return new ExecutionPlanner() {
      @Override
      public Stream stream(String kafkaTopicName, Field windowedGenericRowConsumed) {
        return null;
      }

      @Override
      public Table windowedTable(String kafkaTopicName, Topology.AutoOffsetReset autoOffsetReset) {
        return null;
      }

      @Override
      public Table table(String kafkaTopicName, Topology.AutoOffsetReset autoOffsetReset) {
        return null;
      }
    };
  }


  @Test
  public void testSimpleLeftJoinLogicalPlan() throws Exception {
    String simpleQuery = "SELECT t1.col1, t2.col1, t1.col4, t2.col2 FROM test1 t1 LEFT JOIN test2 t2 ON t1.col1 = t2.col1;";
    PlanNode logicalPlan = buildLogicalPlan(simpleQuery);

    NewPhysicalPlanBuilder newPhysicalPlanBuilder = new NewPhysicalPlanBuilder();
    SamplePhysicalPlanBuildingStrategy samplePhysicalPlanBuildingStrategy = new
        SamplePhysicalPlanBuildingStrategy();

    Pair<PhysicalPlanNode, Double> selectedPhysicalPlanNode = newPhysicalPlanBuilder
        .getPhysicalPlan
        (logicalPlan, samplePhysicalPlanBuildingStrategy);



    /**
     *  Now that we have the final physical plan with the minimum cost we can just call the
     *  buildExecutionPlan method to generate the Execution plan for the destination system.
     */

    ExecutionPlan executionPlan = selectedPhysicalPlanNode.getLeft().buildExecutionPlan
        (getKafkaStreamsExecutionPlanner());

  }

}
