/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static org.apache.kafka.common.config.ConfigResource.Type.TOPIC;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tesla.data.enforcer.UnmanagedPrefixes;

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

  // Stub the admin client: 'listed' are all topic names on the cluster, 'described' the names that
  // listExisting is expected to describe. Describing any other set of names returns null and fails the test.
  private void stubCluster(Set<String> listed, Set<String> described) {
    Cluster cluster = createCluster(1);
    TopicPartitionInfo tp = new TopicPartitionInfo(0, cluster.nodeById(0), cluster.nodes(), Collections.emptyList());
    KafkaFuture<Config> kfc =
        KafkaFuture.completedFuture(new Config(Collections.singletonList(new ConfigEntry("k", "v"))));
    Map<String, TopicDescription> tds = new HashMap<>();
    Map<ConfigResource, KafkaFuture<Config>> configs = new HashMap<>();
    for (String name : described) {
      tds.put(name, new TopicDescription(name, false, Collections.singletonList(tp)));
      configs.put(new ConfigResource(TOPIC, name), kfc);
    }
    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
    when(listTopicsResult.names()).thenReturn(KafkaFuture.completedFuture(listed));
    when(describeTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(tds));
    when(describeConfigsResult.values()).thenReturn(configs);
    when(adminClient.listTopics(any(ListTopicsOptions.class))).thenReturn(listTopicsResult);
    when(adminClient.describeTopics(described)).thenReturn(describeTopicsResult);
    when(adminClient.describeConfigs(any(Collection.class))).thenReturn(describeConfigsResult);
  }

  @Test
  public void testListExisting() {
    // '_c' is internal and must not even be described
    stubCluster(Set.of("a", "b", "_c"), Set.of("a", "b"));
    TopicService service = new TopicServiceImpl(adminClient, true);
    Assert.assertEquals(Set.of("a", "b"), service.listExisting().keySet());
  }

  @Test
  public void testListExistingSkipsUnmanagedTopics() {
    stubCluster(Set.of("a", "ext-b", "vendor-c", "_d"), Set.of("a"));
    UnmanagedPrefixes unmanaged = UnmanagedPrefixes.from(Map.of("topicPrefixes", List.of("ext-", "vendor-")));
    TopicService service = new TopicServiceImpl(adminClient, true, unmanaged);

    Assert.assertEquals(Set.of("a"), service.listExisting().keySet());

    // unmanaged topics are never described, the enforcer may lack describe rights on them
    verify(adminClient).describeTopics(Set.of("a"));
    ArgumentCaptor<Collection> resources = ArgumentCaptor.forClass(Collection.class);
    verify(adminClient).describeConfigs(resources.capture());
    Assert.assertEquals(Set.of(new ConfigResource(TOPIC, "a")), new HashSet<>(resources.getValue()));
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
  public void testCreateInBatches() {
    TopicService service = new TopicServiceImpl(adminClient, true);
    CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
    when(createTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(null));
    when(adminClient.createTopics(any(Collection.class),
        any(CreateTopicsOptions.class))).thenReturn(createTopicsResult);

    service.create(List.of(
        new ConfiguredTopic("g1t1", 7_000, (short) 2, Collections.emptyMap()),
        new ConfiguredTopic("g1t2", 1_000, (short) 2, Collections.emptyMap()),
        new ConfiguredTopic("g2", 7_000, (short) 2, Collections.emptyMap()),
        new ConfiguredTopic("g3", 2_000, (short) 2, Collections.emptyMap()),
        new ConfiguredTopic("g4", 10_000, (short) 2, Collections.emptyMap())));

    ArgumentCaptor<List<NewTopic>> newTopics = ArgumentCaptor.forClass(List.class);
    verify(adminClient, times(4)).createTopics(newTopics.capture(), any());

    List<List<String>> topicBatches = newTopics.getAllValues().stream().map(
        l -> l.stream().map(NewTopic::name).toList()).toList();
    Assert.assertEquals(2, topicBatches.get(0).size());
    Assert.assertArrayEquals(new String[]{"g1t1", "g1t2"}, topicBatches.get(0).toArray());
    Assert.assertEquals(1, topicBatches.get(1).size());
    Assert.assertArrayEquals(new String[]{"g2"}, topicBatches.get(1).toArray());
    Assert.assertEquals(1, topicBatches.get(2).size());
    Assert.assertArrayEquals(new String[]{"g3"}, topicBatches.get(2).toArray());
    Assert.assertEquals(1, topicBatches.get(3).size());
    Assert.assertArrayEquals(new String[]{"g4"}, topicBatches.get(3).toArray());
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