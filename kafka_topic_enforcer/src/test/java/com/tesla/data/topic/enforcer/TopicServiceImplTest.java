/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.AlterConfigsResult;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreatePartitionsResult;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import tesla.shade.com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TopicServiceImplTest {

  private KafkaAdminClient adminClient;

  @Before
  public void setup() {
    adminClient = mock(KafkaAdminClient.class);
  }

  @Test
  public void testConfiguredTopic() {
    Cluster cluster = createCluster(1);
    TopicPartitionInfo tp = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.emptyList());
    TopicDescription td = new TopicDescription("test", false, Collections.singletonList(tp));
    ConfigEntry configEntry = mock(ConfigEntry.class);
    when(configEntry.source()).thenReturn(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);
    KafkaFuture<Config> kfc = KafkaFuture.completedFuture(new Config(Collections.singletonList(configEntry)));
    ConfiguredTopic expected = new ConfiguredTopic("test", 1, (short) 1, Collections.emptyMap());
    Assert.assertEquals(expected, TopicServiceImpl.configuredTopic(td, kfc));
  }

  @Test
  public void testIsNonDefault() {
    ConfigEntry configEntry = mock(ConfigEntry.class);
    when(configEntry.source()).thenReturn(ConfigEntry.ConfigSource.DYNAMIC_DEFAULT_BROKER_CONFIG);
    Assert.assertFalse(TopicServiceImpl.isNonDefault(configEntry));
  }

  @Test
  public void testIsDefault() {
    ConfigEntry configEntry = mock(ConfigEntry.class);
    when(configEntry.source()).thenReturn(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
    Assert.assertTrue(TopicServiceImpl.isNonDefault(configEntry));
  }


  private Cluster createCluster(int numNodes) {
    Map<Integer, Node> nodes = new HashMap<>();
    for (int i = 0; i < numNodes; ++i) {
      nodes.put(i, new Node(i, "localhost", 9092 + i));
    }
    return new Cluster("mockClusterId", nodes.values(),
        Collections.emptySet(), Collections.emptySet(),
        Collections.emptySet(), nodes.get(0));
  }

  @Test
  public void testListExisting() {
    Cluster cluster = createCluster(1);
    TopicPartitionInfo tp = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.emptyList());
    ConfigEntry configEntry = new ConfigEntry("k", "v");
    KafkaFuture<Config> kfc = KafkaFuture.completedFuture(new Config(Collections.singletonList(configEntry)));
    Set<String> topicNames = new HashSet<>(Arrays.asList("a", "b", "_c"));
    Map<String, TopicDescription> tds = ImmutableMap.of(
        "a", new TopicDescription("a", false, Collections.singletonList(tp)),
        "b", new TopicDescription("b", false, Collections.singletonList(tp)),
        "c", new TopicDescription("_c", false, Collections.singletonList(tp))
    );
    Map<ConfigResource, KafkaFuture<Config>> configs = ImmutableMap.of(
        new ConfigResource(TOPIC, "a"), kfc,
        new ConfigResource(TOPIC, "b"), kfc,
        new ConfigResource(TOPIC, "_c"), kfc
    );

    TopicService service = new TopicServiceImpl(adminClient, true);
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);

    when(describeTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(tds));
    when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(topicNames));
    when(describeConfigsResult.values()).thenReturn(configs);
    when(adminClient.listTopics(any(ListTopicsOptions.class))).thenReturn(listTopicsResult);
    when(adminClient.describeTopics(topicNames)).thenReturn(describeTopicsResult);
    when(adminClient.describeConfigs(any(Collection.class))).thenReturn(describeConfigsResult);

    Map<String, ConfiguredTopic> actual = service.listExisting();
    Assert.assertEquals(2, actual.size());
    Assert.assertEquals(new HashSet<>(Arrays.asList("a", "b")), actual.keySet());
  }

  @Test
  public void testCreate() {
    TopicService service = new TopicServiceImpl(adminClient, true);
    CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
    when(createTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.createTopics(any(Collection.class),
        any(CreateTopicsOptions.class))).thenReturn(createTopicsResult);

    service.create(Collections.singletonList(
        new ConfiguredTopic("test", 1, (short) 2, Collections.emptyMap())));

    ArgumentCaptor<List> newTopics = ArgumentCaptor.forClass(List.class);
    ArgumentCaptor<CreateTopicsOptions> options = ArgumentCaptor.forClass(CreateTopicsOptions.class);
    verify(adminClient).createTopics((Collection<NewTopic>) newTopics.capture(), options.capture());
    Assert.assertEquals(1, newTopics.getValue().size());
    Assert.assertEquals("test", ((NewTopic) newTopics.getValue().get(0)).name());
    Assert.assertEquals(2, ((NewTopic) newTopics.getValue().get(0)).replicationFactor());
    Assert.assertTrue(options.getValue().shouldValidateOnly());
  }

  @Test
  public void testTopicConfig() {
    ConfiguredTopic emptyConfig = new ConfiguredTopic("test", 1, (short) 1, Collections.emptyMap());
    Assert.assertTrue(TopicServiceImpl.resourceConfig(emptyConfig).entries().isEmpty());

    ConfiguredTopic notEmptyConfig = new ConfiguredTopic("test", 1, (short) 1, Collections.singletonMap("k", "v"));
    Assert.assertEquals(1, TopicServiceImpl.resourceConfig(notEmptyConfig).entries().size());
    Assert.assertEquals("v", TopicServiceImpl.resourceConfig(notEmptyConfig).get("k").value());
    Assert.assertFalse(TopicServiceImpl.resourceConfig(notEmptyConfig).get("k").isDefault());
  }

  @Test
  public void testIncreasePartitions() {
    TopicService service = new TopicServiceImpl(adminClient, true);
    CreatePartitionsResult result = mock(CreatePartitionsResult.class);
    when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.createPartitions(any(Map.class), any(CreatePartitionsOptions.class))).thenReturn(result);

    service.increasePartitions(Collections.singletonList(
        new ConfiguredTopic("test", 3, (short) 2, Collections.emptyMap())));

    ArgumentCaptor<Map> increase = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<CreatePartitionsOptions> options = ArgumentCaptor.forClass(CreatePartitionsOptions.class);
    verify(adminClient).createPartitions((Map<String, NewPartitions>) increase.capture(), options.capture());
    Assert.assertEquals(1, increase.getValue().size());
    Assert.assertTrue(increase.getValue().containsKey("test"));
    Assert.assertEquals(3, ((NewPartitions) increase.getValue().get("test")).totalCount());
    Assert.assertTrue(options.getValue().validateOnly());
  }

  @Test
  public void testAlterConfiguration() {
    TopicService service = new TopicServiceImpl(adminClient, true);
    AlterConfigsResult result = mock(AlterConfigsResult.class);
    when(result.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.alterConfigs(any(Map.class), any(AlterConfigsOptions.class))).thenReturn(result);

    service.alterConfiguration(Collections.singletonList(
        new ConfiguredTopic("test", 3, (short) 2, Collections.singletonMap("k", "v"))));

    ArgumentCaptor<Map> alter = ArgumentCaptor.forClass(Map.class);
    ArgumentCaptor<AlterConfigsOptions> options = ArgumentCaptor.forClass(AlterConfigsOptions.class);
    verify(adminClient).alterConfigs((Map<ConfigResource, Config>) alter.capture(), options.capture());
    Assert.assertEquals(1, alter.getValue().size());
    ConfigResource expectedKey = new ConfigResource(TOPIC, "test");
    Assert.assertTrue(alter.getValue().containsKey(expectedKey));
    Assert.assertEquals("v", ((Config) alter.getValue().get(expectedKey)).get("k").value());
    Assert.assertTrue(options.getValue().shouldValidateOnly());
  }
}