/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udf.casestatement;

import java.util.List;

public final class SearchedCasedStatementFunction {

  public static Object searchedCasedStatementFunction(
      final List<Boolean> whenList,
      final List thenList,
      final Object defaultValue) {
    for (int i = 0; i < whenList.size(); i++) {
      if (whenList.get(i)) {
        return thenList.get(i);
      }
    }
    return defaultValue;
  }

}
