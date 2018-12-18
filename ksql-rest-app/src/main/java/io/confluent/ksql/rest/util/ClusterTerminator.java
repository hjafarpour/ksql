/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.util;

import io.confluent.ksql.KsqlEngine;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.rest.server.KsqlRestConfig;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.util.ExecutorUtil;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.PersistentQueryMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

public class ClusterTerminator {

  private final KsqlConfig ksqlConfig;
  private final KsqlEngine ksqlEngine;
  private final ServiceContext serviceContext;

  public ClusterTerminator(
      final KsqlConfig ksqlConfig,
      final KsqlEngine ksqlEngine,
      final ServiceContext serviceContext
  ) {
    Objects.requireNonNull(ksqlConfig, "ksqlConfig is null.");
    Objects.requireNonNull(ksqlEngine, "ksqlEngine is null.");
    this.ksqlConfig = ksqlConfig;
    this.ksqlEngine = ksqlEngine;
    this.serviceContext = Objects.requireNonNull(serviceContext);
  }

  @SuppressWarnings("unchecked")
  public void terminateCluster(final List<String> deleteTopicPatterns) {
    validateDeleteList(deleteTopicPatterns);
    terminatePersistentQueries();
    deleteSinkTopics(deleteTopicPatterns);
    deleteCommandTopic();
    ksqlEngine.close();
  }

  private void terminatePersistentQueries() {
    new ArrayList<>(ksqlEngine.getPersistentQueries()).stream()
        .map(PersistentQueryMetadata::getQueryId)
        .forEach(queryId -> ksqlEngine.terminateQuery(queryId, true));
  }

  private void validateDeleteList(final List<String> deleteTopicPatterns) {
    if (deleteTopicPatterns.isEmpty()) {
      return;
    }
    ksqlEngine.getMetaStore().getAllKsqlTopics().values()
        .forEach(ksqlTopic -> deleteTopicPatterns.forEach(
            deleteTopicPattern -> {
              if (ksqlTopic.getKafkaTopicName().equals(deleteTopicPattern)
                  && !ksqlTopic.isKsqlSink()) {
                throw new KsqlException("Invalid request: " + deleteTopicPattern
                    + " is not a KSQL sink topic.");
              }
            }
        ));
  }

  private void deleteSinkTopics(final List<String> deleteTopicPatterns) {
    if (deleteTopicPatterns.isEmpty()) {
      return;
    }

    final List<Pattern> patterns = deleteTopicPatterns.stream()
        .map(Pattern::compile)
        .collect(Collectors.toList());

    final MetaStore metaStore = ksqlEngine.getMetaStore();
    final List<String> toDelete = metaStore.getAllKsqlTopics().values().stream()
        .filter(KsqlTopic::isKsqlSink)
        .map(KsqlTopic::getKafkaTopicName)
        .filter(topicName -> topicShouldBeDeleted(topicName, patterns))
        .collect(Collectors.toList());

    try {
      ExecutorUtil.executeWithRetries(
          () -> serviceContext.getTopicClient().deleteTopics(toDelete),
          ExecutorUtil.RetryBehaviour.ALWAYS);
    } catch (final Exception e) {
      throw new KsqlException(
          "Exception while deleting topics: " + StringUtils.join(toDelete, ","));
    }
  }

  private static boolean topicShouldBeDeleted(
      final String topicName, final List<Pattern> patterns
  ) {
    return patterns.stream()
        .anyMatch(pattern -> pattern.matcher(topicName).matches());
  }

  private void deleteCommandTopic() {
    final String ksqlServiceId = ksqlConfig.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
    final String commandTopic = KsqlRestConfig.getCommandTopic(ksqlServiceId);
    final KafkaTopicClient kafkaTopicClient = serviceContext.getTopicClient();
    try {
      ExecutorUtil.executeWithRetries(
          () -> kafkaTopicClient.deleteTopics(
              Collections.singletonList(commandTopic)),
          ExecutorUtil.RetryBehaviour.ALWAYS);
    } catch (final Exception e) {
      throw new KsqlException("Could not delete the command topic: "
          + commandTopic, e);
    }
  }
}
