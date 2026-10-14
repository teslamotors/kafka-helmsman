/*
 * Copyright (c) 2026. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tesla.data.topic.enforcer.RetentionMetrics.ActualConfigs;

import io.prometheus.client.CollectorRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class RetentionMetricsTest {

  private static Double exported(String topic) {
    return CollectorRegistry.defaultRegistry.getSampleValue(
        "kafka_topic_enforcer_topic_retention_seconds",
        new String[] {"topic"},
        new String[] {topic});
  }

  private static RetentionMetrics metrics(Supplier<Map<String, Map<String, String>>> actual) {
    return new RetentionMetrics(() -> new ActualConfigs(actual.get(), Map.of()));
  }

  @Test
  public void testRetentionFromTheBrokerConfig() {
    Map<String, Double> retention = RetentionMetrics.retentionSeconds(
        Map.of("a", Map.of("retention.ms", "86400000", "cleanup.policy", "delete")));
    assertEquals(Map.of("a", 86400.0), retention);
  }

  @Test
  public void testTopicsThatNeverExpireByTimeAreSkipped() {
    Map<String, Double> retention = RetentionMetrics.retentionSeconds(
        Map.of(
            "infinite", Map.of("retention.ms", "-1", "cleanup.policy", "delete"),
            "compacted", Map.of("retention.ms", "604800000", "cleanup.policy", "compact"),
            "compacted_and_deleted", Map.of("retention.ms", "604800000", "cleanup.policy", "compact,delete"),
            "no_retention_ms", Map.of("cleanup.policy", "delete"),
            "unknown", Map.of()));
    assertEquals(Map.of("compacted_and_deleted", 604800.0), retention);
  }

  @Test
  public void testUpdateExportsRetentionAndDropsRemovedTopics() {
    AtomicReference<Map<String, Map<String, String>>> actual = new AtomicReference<>(Map.of(
        "stats_a", Map.of("retention.ms", "86400000"),
        "stats_b", Map.of("retention.ms", "259200000")));
    RetentionMetrics metrics = metrics(actual::get);

    metrics.update();
    assertEquals(86400.0, exported("stats_a"), 0.0);
    assertEquals(259200.0, exported("stats_b"), 0.0);

    actual.set(Map.of("stats_a", Map.of("retention.ms", "172800000")));
    metrics.update();
    assertEquals(172800.0, exported("stats_a"), 0.0);
    assertNull(exported("stats_b"));
  }

  @Test
  public void testUpdateKeepsLastRetentionWhenConfigsCannotBeRead() {
    AtomicBoolean brokersDown = new AtomicBoolean(false);
    RetentionMetrics metrics = metrics(() -> {
      if (brokersDown.get()) {
        throw new IllegalStateException("brokers down");
      }
      return Map.of("failing", Map.of("retention.ms", "86400000"));
    });
    metrics.update();

    brokersDown.set(true);
    metrics.update();
    assertEquals(86400.0, exported("failing"), 0.0);
  }

  @Test
  public void testActualConfigsSkipInternalTopics() {
    Node node = new Node(0, "localhost", 9092);
    List<TopicPartitionInfo> partitions = List.of(new TopicPartitionInfo(0, node, List.of(node), List.of(node)));
    MockAdminClient adminClient = new MockAdminClient(List.of(node), node);
    adminClient.addTopic(false, "events", partitions, Map.of("retention.ms", "86400000"));
    adminClient.addTopic(false, "_schemas", partitions, Map.of("cleanup.policy", "compact"));
    adminClient.addTopic(true, "__consumer_offsets", partitions, Map.of("cleanup.policy", "compact"));

    assertEquals(
        new ActualConfigs(Map.of("events", Map.of("retention.ms", "86400000")), Map.of()),
        RetentionMetrics.actualConfigs(adminClient));
  }

  @Test
  public void testActualConfigsReportTopicsWhoseConfigCannotBeRead() throws Exception {
    AdminClient adminClient = mock(AdminClient.class);
    ListTopicsResult topics = mock(ListTopicsResult.class);
    when(topics.names()).thenReturn(KafkaFuture.completedFuture(Set.of("ok", "slow")));
    when(adminClient.listTopics(any(ListTopicsOptions.class))).thenReturn(topics);
    KafkaFutureImpl<Config> slow = new KafkaFutureImpl<>();
    slow.completeExceptionally(new TimeoutException("describe timed out"));
    DescribeConfigsResult described = mock(DescribeConfigsResult.class);
    when(described.values()).thenReturn(Map.of(
        new ConfigResource(ConfigResource.Type.TOPIC, "ok"),
        KafkaFuture.completedFuture(new Config(List.of(new ConfigEntry("retention.ms", "86400000")))),
        new ConfigResource(ConfigResource.Type.TOPIC, "slow"), slow));
    when(adminClient.describeConfigs(anyCollection())).thenReturn(described);

    ActualConfigs actual = RetentionMetrics.actualConfigs(adminClient);
    assertEquals(Map.of("ok", Map.of("retention.ms", "86400000"), "slow", Map.of()), actual.configs());
    assertEquals(Set.of("slow"), actual.failedTopics().keySet());
    assertTrue(actual.failedTopics().get("slow").contains("describe timed out"));
  }

  @Test
  public void testUpdateKeepsLastRetentionOfTopicsWhoseConfigCannotBeRead() {
    AtomicReference<ActualConfigs> actual = new AtomicReference<>(new ActualConfigs(Map.of(
        "unread_no_desired", Map.of("retention.ms", "86400000"),
        "unread_desired", Map.of("retention.ms", "86400000")), Map.of()));
    RetentionMetrics metrics = new RetentionMetrics(actual::get);
    metrics.update();

    actual.set(new ActualConfigs(
        Map.of("unread_no_desired", Map.of(), "unread_desired", Map.of()),
        Map.of("unread_no_desired", "timeout", "unread_desired", "timeout")));
    metrics.update();
    // the last retention the brokers reported, not the desired one: the brokers may not have applied it (dry run,
    // unsafe drift in safe mode)
    assertEquals(86400.0, exported("unread_no_desired"), 0.0);
    assertEquals(86400.0, exported("unread_desired"), 0.0);

    // once the topic is gone (no longer listed), its series goes too
    actual.set(new ActualConfigs(Map.of(), Map.of()));
    metrics.update();
    assertNull(exported("unread_no_desired"));
  }
}
