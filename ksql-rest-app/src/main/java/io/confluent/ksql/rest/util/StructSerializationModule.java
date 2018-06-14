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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;

public class StructSerializationModule extends StdSerializer<Struct> {

  public StructSerializationModule() {
    super(Struct.class);
  }

  @Override
  public void serialize(Struct struct, JsonGenerator jsonGenerator,
      SerializerProvider serializerProvider) throws IOException {
    jsonGenerator.writeStartObject();
    for (Field field: struct.schema().fields()) {
      if (field.schema().type() == Type.STRUCT) {
        jsonGenerator.writeStartObject();
        serialize((Struct) struct.get(field.name()), jsonGenerator, serializerProvider);
        jsonGenerator.writeEndObject();
      } else {
        jsonGenerator.writeStartObject();
        jsonGenerator.writeObject(struct.get(field.name()));
        jsonGenerator.writeEndObject();
      }
    }

  }
}
