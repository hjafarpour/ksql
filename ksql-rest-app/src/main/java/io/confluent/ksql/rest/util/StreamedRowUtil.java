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
 */

package io.confluent.ksql.rest.util;

import io.confluent.ksql.GenericRow;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;

public class StreamedRowUtil {

  public static GenericRow getNewUpdatedRow(GenericRow genericRow, JsonConverter jsonConverter) {

    return new GenericRow(
        genericRow.getColumns().stream()
            .map(obj -> getValidObject(obj, jsonConverter))
            .collect(Collectors.toList())
    );
  }

  private static Object getValidObject(Object object, JsonConverter jsonConverter) {
    if (object == null) {
      return null;
    }
    if (object instanceof Struct) {
      final Struct struct = (Struct) object;
      return new String(jsonConverter.fromConnectData("", struct.schema(), struct));
    } else if (object instanceof List) {
      List<Object> rowList = (List<Object>) object;
      StringBuilder stringBuilder = new StringBuilder("[");
      stringBuilder.append(
          rowList.stream()
              .map(listItem -> getValidObject(listItem, jsonConverter).toString())
              .collect(Collectors.joining(", "))
      );
      return stringBuilder.append("]").toString();
    } else if (object instanceof Map) {
      Map<String, Object> rowMap = (Map<String, Object>) object;
      StringBuilder stringBuilder = new StringBuilder("{");
      stringBuilder.append(rowMap.entrySet().stream()
          .map(mapEntry -> "\"" + mapEntry.getKey() + "\":"
              + "{" + getValidObject(mapEntry.getValue(), jsonConverter) + "}")
          .collect(Collectors.joining(", ")));
      return stringBuilder.append("}").toString();
    } else {
      return object;
    }
  }

}
