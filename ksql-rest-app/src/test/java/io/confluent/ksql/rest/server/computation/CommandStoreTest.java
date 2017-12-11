/**
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

package io.confluent.ksql.rest.server.computation;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.ksql.metastore.MetaStoreImpl;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.util.Pair;

import static org.easymock.EasyMock.anyLong;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;


@RunWith(EasyMockRunner.class)
public class CommandStoreTest {

  private static final String COMMAND_TOPIC = "command";
  @Mock(type = MockType.NICE)
  private Consumer<CommandId, Command> commandConsumer;
  @Mock(type = MockType.NICE)
  private Producer<CommandId, Command> commandProducer;


  @Test
  public void shouldUseFirstCommandForSameIdIfNoDropBetweenThem() {
    final CommandId commandId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
    final Command originalCommand = new Command("some statement", Collections.emptyMap());
    final Command latestCommand = new Command("a new statement", Collections.emptyMap());
    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
        new ConsumerRecord<>("topic", 0, 0, commandId,
            originalCommand),
        new ConsumerRecord<>("topic", 0, 0, commandId,
            latestCommand))
    ));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());

    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final CommandStore command = new CommandStore(COMMAND_TOPIC, commandConsumer, commandProducer, new CommandIdAssigner(new MetaStoreImpl()));
    final Map<CommandId, Command> commands = getPriorCommands(command);
    assertThat(commands, equalTo(Collections.singletonMap(commandId, originalCommand)));
  }

  @Test
  public void shouldReplaceCommandWithNewCommandAfterDrop() {
    final CommandId createId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
    final CommandId dropId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.DROP);
    final Command originalCommand = new Command("some statement", Collections.emptyMap());
    final Command dropCommand = new Command("drop", Collections.emptyMap());
    final Command latestCommand = new Command("a new statement", Collections.emptyMap());

    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
        new ConsumerRecord<>("topic", 0, 0, createId, originalCommand),
        new ConsumerRecord<>("topic", 0, 0, dropId, dropCommand),
        new ConsumerRecord<>("topic", 0, 0, createId, latestCommand))
    ));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());

    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final CommandStore command = new CommandStore(COMMAND_TOPIC, commandConsumer, commandProducer, new CommandIdAssigner(new MetaStoreImpl()));
    final Map<CommandId, Command> commands = getPriorCommands(command);
    assertThat(commands, equalTo(Collections.singletonMap(createId, latestCommand)));
  }

  @Test
  public void shouldRemoveCreateCommandIfItHasBeenDropped() {
    final CommandId createId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.CREATE);
    final CommandId dropId = new CommandId(CommandId.Type.TABLE, "one", CommandId.Action.DROP);
    final Command originalCommand = new Command("some statement", Collections.emptyMap());
    final Command dropCommand = new Command("drop", Collections.emptyMap());

    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(Collections.singletonMap(new TopicPartition("topic", 0), Arrays.asList(
        new ConsumerRecord<>("topic", 0, 0, createId, originalCommand),
        new ConsumerRecord<>("topic", 0, 0, dropId, dropCommand)
    )));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());

    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final CommandStore commandStore = new CommandStore(COMMAND_TOPIC, commandConsumer, commandProducer, new CommandIdAssigner(new MetaStoreImpl()));
    assertThat(getPriorCommands(commandStore), equalTo(Collections.emptyMap()));
  }

  @Test
  public void shouldCollectTerminatedQueries() {
    final CommandId terminated = new CommandId(CommandId.Type.TERMINATE, "queryId", CommandId.Action.EXECUTE);
    final ConsumerRecords<CommandId, Command> records = new ConsumerRecords<>(
        Collections.singletonMap(new TopicPartition("topic", 0), Collections.singletonList(
        new ConsumerRecord<>("topic", 0, 0, terminated, new Command("terminate query 'queryId'", Collections.emptyMap()))
    )));

    EasyMock.expect(commandConsumer.partitionsFor(COMMAND_TOPIC)).andReturn(Collections.emptyList());
    EasyMock.expect(commandConsumer.poll(anyLong())).andReturn(records)
        .andReturn(new ConsumerRecords<>(Collections.emptyMap()));
    EasyMock.replay(commandConsumer);

    final CommandStore commandStore = new CommandStore(COMMAND_TOPIC, commandConsumer, commandProducer, new CommandIdAssigner(new MetaStoreImpl()));
    final RestoreCommands restoreCommands = commandStore.getRestoreCommands();
    assertThat(restoreCommands.terminatedQueries(), equalTo(Collections.singletonMap(new QueryId("queryId"), terminated)));
  }

  private Map<CommandId, Command> getPriorCommands(CommandStore command) {
    final RestoreCommands priorCommands = command.getRestoreCommands();
    final Map<CommandId, Command> commands = new HashMap<>();
    priorCommands.forEach(((id, cmd, terminatedQueries) -> commands.put(id, cmd)));
    return commands;
  }

}