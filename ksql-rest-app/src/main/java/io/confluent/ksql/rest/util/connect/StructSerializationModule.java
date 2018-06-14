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

package io.confluent.ksql.rest.util.connect;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Preconditions;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructSerializationModule extends SimpleModule {

  private static final Logger LOG = LoggerFactory.getLogger(StructSerializationModule.class);
  private static final JsonConverter jsonConverter = new JsonConverter();

  public StructSerializationModule() {
    super();
    jsonConverter.configure(Collections.singletonMap("schemas.enable", false), false);
    addSerializer(Struct.class, new Serializer());
  }

  static class Serializer extends JsonSerializer<Struct> {

    ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public void serialize(
        Struct struct,
        JsonGenerator jsonGenerator,
        SerializerProvider serializerProvider
    ) throws IOException {
      struct.validate();
//      Storage result = new Storage();
//      result.schema = struct.schema();
//      result.fieldValues = new ArrayList<>();
//      for (Field field : struct.schema().fields()) {
//        LOG.trace("serialize() - Processing field '{}'", field.name());
//        FieldValue fieldValue = new FieldValue();
//        fieldValue.name = field.name();
//        fieldValue.schema = field.schema();
//        fieldValue.value(struct.get(field));
//        result.fieldValues.add(fieldValue);
//      }
//      jsonGenerator.writeObject(result);

      jsonGenerator.writeObject(objectMapper.readTree(jsonConverter.fromConnectData("", struct.schema(), struct)));
    }
  }

}
