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

package io.confluent.ksql.function.udf.string;

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Kudf;

public class IfNullKudf implements Kudf {

  @Override
  public Object evaluate(final Object... args) {
    if (args.length != 2) {
      throw new KsqlFunctionException("IfNull udf should have two input argument.");
    }
    if (args[0] == null) {
      return args[1];
    } else {
      return args[0];
    }
  }
}
