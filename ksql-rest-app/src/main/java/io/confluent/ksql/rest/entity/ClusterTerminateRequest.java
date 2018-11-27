/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class ClusterTerminateRequest {

  private final List<String> deleteTopicList;

  public ClusterTerminateRequest(
      @JsonProperty("deleteTopicList") final List<String> deleteTopicList
  ) {
    this.deleteTopicList = deleteTopicList == null
        ? Collections.emptyList()
        : ImmutableList.copyOf(deleteTopicList);
  }

  public List<String> getDeleteTopicList() {
    return deleteTopicList;
  }

  @JsonIgnore
  public Map<String, Object> getStreamsProperties() {
    return Collections.singletonMap("deleteTopicList", deleteTopicList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(deleteTopicList);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof ClusterTerminateRequest)) {
      return false;
    }
    final ClusterTerminateRequest that = (ClusterTerminateRequest) o;
    return Objects.equals(deleteTopicList, that.getDeleteTopicList());
  }
}