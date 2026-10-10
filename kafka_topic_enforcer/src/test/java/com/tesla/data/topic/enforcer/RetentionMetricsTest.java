/*
 * Copyright (c) 2026. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tesla.data.topic.enforcer.RetentionMetrics.ActualConfigs;
import com.tesla.data.topic.enforcer.RetentionMetrics.ExpiredRecords;
import com.tesla.data.topic.enforcer.RetentionMetrics.GroupPartition;
import com.tesla.data.topic.enforcer.RetentionMetrics.GroupTopic;
import com.tesla.data.topic.enforcer.RetentionMetrics.Options;

import io.prometheus.client.CollectorRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.CoordinatorNotAvailableException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RetentionMetricsTest {

  private static final Pattern GROUPS = Pattern.compile("app\\..*");

  private static Double exported(String topic) {
    return CollectorRegistry.defaultRegistry.getSampleValue(
        "kafka_topic_enforcer_topic_retention_seconds",
        new String[] {"topic"},
        new String[] {topic});
  }

  private static Double expired(String group, String topic) {
    return CollectorRegistry.defaultRegistry.getSampleValue(
        "kafka_topic_enforcer_consumer_expired_records",
        new String[] {"consumer", "topic"},
        new String[] {group, topic});
  }

  private static RetentionMetrics metrics(
      Supplier<Map<String, Map<String, String>>> actual, Supplier<ExpiredRecords> expired) {
    return new RetentionMetrics(() -> new ActualConfigs(actual.get(), Map.of()), expired);
  }

  private static RetentionMetrics metrics(Supplier<Map<String, Map<String, String>>> actual) {
    return metrics(actual, () -> new ExpiredRecords(Map.of(), Map.of(), Map.of()));
  }

  private static ConsumerGroupDescription group(String id, ConsumerGroupState state, TopicPartition... assigned) {
    List<MemberDescription> members = assigned.length == 0 ? List.of() :
        List.of(new MemberDescription("member", "client", "host", new MemberAssignment(Set.of(assigned))));
    return new ConsumerGroupDescription(id, false, members, "range", state, new Node(0, "localhost", 9092));
  }

  private static void describes(AdminClient adminClient, ConsumerGroupDescription... groups) {
    Map<String, KafkaFuture<ConsumerGroupDescription>> described = new HashMap<>();
    for (ConsumerGroupDescription group : groups) {
      described.put(group.groupId(), KafkaFuture.completedFuture(group));
    }
    DescribeConsumerGroupsResult result = mock(DescribeConsumerGroupsResult.class);
    when(result.describedGroups()).thenReturn(described);
    when(adminClient.describeConsumerGroups(anyCollection())).thenReturn(result);
  }

  // describeTopics answers with the given partition counts; other topics are unknown (deleted)
  private static void partitions(AdminClient adminClient, Map<String, Integer> counts) {
    partitions(adminClient, counts, Set.of());
  }

  // as above; the leaderless partitions have no leader, which describeTopics reports as a null leader
  private static void partitions(
      AdminClient adminClient, Map<String, Integer> counts, Set<TopicPartition> leaderless) {
    when(adminClient.describeTopics(anyCollection())).thenAnswer(invocation -> {
      Map<String, KafkaFuture<TopicDescription>> described = new HashMap<>();
      for (Object topic : (Collection<?>) invocation.getArgument(0)) {
        String name = (String) topic;
        if (counts.containsKey(name)) {
          List<TopicPartitionInfo> infos = new ArrayList<>();
          for (int p = 0; p < counts.get(name); p++) {
            Node leader = leaderless.contains(new TopicPartition(name, p)) ? null : new Node(1, "broker", 9092);
            infos.add(new TopicPartitionInfo(p, leader, List.of(), List.of()));
          }
          described.put(name, KafkaFuture.completedFuture(new TopicDescription(name, false, infos)));
        } else {
          KafkaFutureImpl<TopicDescription> unknown = new KafkaFutureImpl<>();
          unknown.completeExceptionally(new UnknownTopicOrPartitionException(name));
          described.put(name, unknown);
        }
      }
      return new DescribeTopicsResult(described) {
      };
    });
  }

  private static ListOffsetsResult logStarts(Map<TopicPartition, Long> starts) {
    Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new HashMap<>();
    starts.forEach((partition, start) ->
        futures.put(partition, KafkaFuture.completedFuture(new ListOffsetsResultInfo(start, -1L, Optional.empty()))));
    return new ListOffsetsResult(futures);
  }

  // listOffsets as kafka-clients 2.8 answers it: if any topic in the call is unknown or has a partition without a
  // leader, the client retries its metadata lookup until the API timeout, then fails every partition in the call
  private static void logStartsAsKafka28(
      AdminClient adminClient, Map<TopicPartition, Long> starts, Set<String> unavailableTopics) {
    when(adminClient.listOffsets(anyMap())).thenAnswer(invocation -> {
      Map<TopicPartition, ?> requested = invocation.getArgument(0);
      boolean timesOut = requested.keySet().stream().anyMatch(p -> unavailableTopics.contains(p.topic()));
      Map<TopicPartition, KafkaFuture<ListOffsetsResultInfo>> futures = new HashMap<>();
      for (TopicPartition partition : requested.keySet()) {
        if (timesOut) {
          KafkaFutureImpl<ListOffsetsResultInfo> timedOut = new KafkaFutureImpl<>();
          timedOut.completeExceptionally(new TimeoutException("Timed out waiting for a node assignment."));
          futures.put(partition, timedOut);
        } else {
          futures.put(partition, KafkaFuture.completedFuture(
              new ListOffsetsResultInfo(starts.get(partition), -1L, Optional.empty())));
        }
      }
      return new ListOffsetsResult(futures);
    });
  }

  private static void lists(AdminClient adminClient, String... groups) {
    ListConsumerGroupsResult listed = mock(ListConsumerGroupsResult.class);
    when(listed.all()).thenReturn(KafkaFuture.completedFuture(
        Stream.of(groups).map(group -> new ConsumerGroupListing(group, false)).collect(Collectors.toList())));
    when(adminClient.listConsumerGroups()).thenReturn(listed);
  }

  private static void commits(AdminClient adminClient, String group, Map<TopicPartition, Long> offsets) {
    Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
    offsets.forEach((partition, offset) -> committed.put(partition, new OffsetAndMetadata(offset)));
    ListConsumerGroupOffsetsResult result = mock(ListConsumerGroupOffsetsResult.class);
    when(result.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(committed));
    when(adminClient.listConsumerGroupOffsets(group)).thenReturn(result);
  }

  @Test
  public void testExpiredRecordsSurviveDeletedTopicsAndPartitionsWithoutLeader() {
    TopicPartition healthy = new TopicPartition("healthy", 0);
    TopicPartition offline = new TopicPartition("offline", 0);
    TopicPartition deleted = new TopicPartition("deleted", 0);
    AdminClient adminClient = mock(AdminClient.class);
    lists(adminClient, "app.reads_offline", "app.reads_deleted");
    commits(adminClient, "app.reads_offline", Map.of(healthy, 10L, offline, 10L));
    commits(adminClient, "app.reads_deleted", Map.of(healthy, 20L, deleted, 5L));
    describes(adminClient,
        group("app.reads_offline", ConsumerGroupState.EMPTY), group("app.reads_deleted", ConsumerGroupState.EMPTY));
    partitions(adminClient, Map.of("healthy", 1, "offline", 1), Set.of(offline));
    logStartsAsKafka28(adminClient, Map.of(healthy, 30L, offline, 40L, deleted, 50L), Set.of("offline", "deleted"));

    ExpiredRecords expired = RetentionMetrics.expiredRecords(adminClient, GROUPS, new HashMap<>());
    assertEquals(Map.of(
        new GroupTopic("app.reads_deleted", "healthy"), 10L,
        new GroupTopic("app.reads_offline", "healthy"), 20L), expired.values());
    assertEquals(Map.of(), expired.failedGroups());
    assertEquals(Set.of(new GroupTopic("app.reads_offline", "offline")), expired.failedTopics().keySet());
    // neither is asked for: each would make its call wait for the API timeout
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<TopicPartition, OffsetSpec>> requested = ArgumentCaptor.forClass(Map.class);
    verify(adminClient, atLeastOnce()).listOffsets(requested.capture());
    assertEquals(Set.of("healthy"), requested.getAllValues()
        .stream()
        .flatMap(request -> request.keySet().stream())
        .map(TopicPartition::topic)
        .collect(Collectors.toSet()));
  }

  @Test
  public void testATopicThatLosesItsLeaderAfterBeingDescribedFailsOnlyItself() {
    TopicPartition healthy = new TopicPartition("healthy", 0);
    TopicPartition lost = new TopicPartition("lost_leader", 0);
    AdminClient adminClient = mock(AdminClient.class);
    lists(adminClient, "app.a");
    commits(adminClient, "app.a", Map.of(healthy, 10L, lost, 10L));
    describes(adminClient, group("app.a", ConsumerGroupState.EMPTY));
    // described with a leader, gone by the time its log start offsets are read
    partitions(adminClient, Map.of("healthy", 1, "lost_leader", 1));
    logStartsAsKafka28(adminClient, Map.of(healthy, 30L, lost, 40L), Set.of("lost_leader"));

    ExpiredRecords expired = RetentionMetrics.expiredRecords(adminClient, GROUPS, new HashMap<>());
    assertEquals(Map.of(new GroupTopic("app.a", "healthy"), 20L), expired.values());
    assertEquals(Set.of(new GroupTopic("app.a", "lost_leader")), expired.failedTopics().keySet());
  }

  @Test
  public void testUnreadPartitionsKeepTheirStateWhileTheirTopicCannotBeRead() {
    TopicPartition read = new TopicPartition("events", 0);
    TopicPartition unread = new TopicPartition("events", 1);
    AdminClient adminClient = mock(AdminClient.class);
    lists(adminClient, "app.a");
    commits(adminClient, "app.a", Map.of(read, 10L));
    describes(adminClient, group("app.a", ConsumerGroupState.EMPTY));
    Map<GroupPartition, Long> since = new HashMap<>();

    partitions(adminClient, Map.of("events", 2));
    when(adminClient.listOffsets(anyMap())).thenReturn(logStarts(Map.of(read, 10L, unread, 0L)));
    RetentionMetrics.expiredRecords(adminClient, GROUPS, since);

    // the topic loses a leader for a run: no values, but the unread partition's starting point is kept
    partitions(adminClient, Map.of("events", 2), Set.of(read));
    RetentionMetrics.expiredRecords(adminClient, GROUPS, since);

    partitions(adminClient, Map.of("events", 2));
    when(adminClient.listOffsets(anyMap())).thenReturn(logStarts(Map.of(read, 10L, unread, 40L)));
    assertEquals(Map.of(new GroupTopic("app.a", "events"), 40L),
        RetentionMetrics.expiredRecords(adminClient, GROUPS, since).values());
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
    RetentionMetrics metrics =
        new RetentionMetrics(actual::get, () -> new ExpiredRecords(Map.of(), Map.of(), Map.of()));
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

  @Test
  public void testCreatedOnlyForContinuousRunsWithTheSection() {
    AdminClient adminClient = mock(AdminClient.class);
    Map<String, Object> withSection = Map.of("retentionMetrics", Map.of());
    assertNotNull(RetentionMetrics.fromConfig(adminClient, withSection, true));
    // a one-shot run has no metrics server to export them
    assertNull(RetentionMetrics.fromConfig(adminClient, withSection, false));
    assertNull(RetentionMetrics.fromConfig(adminClient, Map.of("kafka", Map.of()), true));
    // an empty section (`retentionMetrics:` in YAML) turns them on with the defaults
    Map<String, Object> emptySection = new HashMap<>();
    emptySection.put("retentionMetrics", null);
    assertNotNull(RetentionMetrics.fromConfig(adminClient, emptySection, true));
  }

  @Test
  public void testOptionsDefaultToAllGroups() {
    assertTrue(Options.from(Map.of("retentionMetrics", Map.of())).consumerGroups().matcher("any.group").matches());
    Map<String, Object> emptySection = new HashMap<>();
    emptySection.put("retentionMetrics", null);
    assertTrue(Options.from(emptySection).consumerGroups().matcher("any.group").matches());
  }

  @Test
  public void testOptionsFromConfig() {
    Options options = Options.from(Map.of("retentionMetrics", Map.of("consumerGroups", "(legacy_)?app\\..*")));
    for (String group : List.of("app.orders", "legacy_app.events")) {
      assertTrue(group, options.consumerGroups().matcher(group).matches());
    }
    for (String group : List.of("other.consumer", "myapp.x", "app")) {
      assertFalse(group, options.consumerGroups().matcher(group).matches());
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConsumerGroupsMustBeAString() {
    // a YAML list would otherwise become the regular expression "[app.a, app.b]", a character class
    Options.from(Map.of("retentionMetrics", Map.of("consumerGroups", List.of("app.a", "app.b"))));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testOptionsMustBeAMap() {
    Options.from(Map.of("retentionMetrics", "app.*"));
  }

  @Test
  public void testExpiredRecordsSumPartitionsPerGroupAndTopic() {
    TopicPartition t1p0 = new TopicPartition("t1", 0);
    TopicPartition t1p1 = new TopicPartition("t1", 1);
    TopicPartition t2p0 = new TopicPartition("t2", 0);
    TopicPartition deleted = new TopicPartition("deleted", 0);
    Map<GroupTopic, Long> expired = RetentionMetrics.expiredRecords(
        Map.of(
            "app.a", Map.of(t1p0, 10L, t1p1, 50L, t2p0, 5L, deleted, 3L),
            "app.b", Map.of(t1p0, 25L)),
        Map.of(t1p0, 30L, t1p1, 40L, t2p0, 5L));
    assertEquals(
        Map.of(
            new GroupTopic("app.a", "t1"), 20L,
            new GroupTopic("app.a", "t2"), 0L,
            new GroupTopic("app.b", "t1"), 5L),
        expired);
  }

  @Test
  public void testExpiredRecordsFromAdminClient() {
    final TopicPartition read = new TopicPartition("events", 0);
    final TopicPartition notCommitted = new TopicPartition("events", 1);
    final TopicPartition deleted = new TopicPartition("deleted", 0);
    AdminClient adminClient = mock(AdminClient.class);

    ListConsumerGroupsResult groups = mock(ListConsumerGroupsResult.class);
    when(groups.all()).thenReturn(KafkaFuture.completedFuture(List.of(
        new ConsumerGroupListing("app.events", false),
        new ConsumerGroupListing("app.unreadable", false),
        new ConsumerGroupListing("other.events", false))));
    when(adminClient.listConsumerGroups()).thenReturn(groups);

    Map<TopicPartition, OffsetAndMetadata> committed = new HashMap<>();
    committed.put(read, new OffsetAndMetadata(10L));
    committed.put(notCommitted, null);
    committed.put(deleted, new OffsetAndMetadata(3L));
    ListConsumerGroupOffsetsResult offsets = mock(ListConsumerGroupOffsetsResult.class);
    when(offsets.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(committed));
    when(adminClient.listConsumerGroupOffsets("app.events")).thenReturn(offsets);
    KafkaFutureImpl<Map<TopicPartition, OffsetAndMetadata>> unreadable = new KafkaFutureImpl<>();
    unreadable.completeExceptionally(new CoordinatorNotAvailableException("moving"));
    ListConsumerGroupOffsetsResult unreadableOffsets = mock(ListConsumerGroupOffsetsResult.class);
    when(unreadableOffsets.partitionsToOffsetAndMetadata()).thenReturn(unreadable);
    when(adminClient.listConsumerGroupOffsets("app.unreadable")).thenReturn(unreadableOffsets);

    KafkaFutureImpl<ListOffsetsResultInfo> unknownTopic = new KafkaFutureImpl<>();
    unknownTopic.completeExceptionally(new UnknownTopicOrPartitionException("deleted"));
    when(adminClient.listOffsets(anyMap())).thenReturn(new ListOffsetsResult(Map.of(
        read, KafkaFuture.completedFuture(new ListOffsetsResultInfo(30L, -1L, Optional.empty())),
        notCommitted, KafkaFuture.completedFuture(new ListOffsetsResultInfo(7L, -1L, Optional.empty())),
        deleted, unknownTopic)));
    describes(adminClient, group("app.events", ConsumerGroupState.EMPTY));
    partitions(adminClient, Map.of("events", 2));

    ExpiredRecords expired = RetentionMetrics.expiredRecords(adminClient, GROUPS, new HashMap<>());
    assertEquals(Map.of(new GroupTopic("app.events", "events"), 20L), expired.values());
    assertEquals(Set.of("app.unreadable"), expired.failedGroups().keySet());
    verify(adminClient, never()).listConsumerGroupOffsets("other.events");
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Map<TopicPartition, OffsetSpec>> requested = ArgumentCaptor.forClass(Map.class);
    // only the existing topic is asked for: one call per topic, none for the deleted one
    verify(adminClient).listOffsets(requested.capture());
    assertEquals(Set.of(read, notCommitted), requested.getValue().keySet());
  }

  @Test
  public void testExpiredRecordsReportTopicsWhoseLogStartOffsetsCannotBeRead() {
    final TopicPartition leaderless = new TopicPartition("leaderless", 0);
    final TopicPartition healthy = new TopicPartition("healthy", 0);
    AdminClient adminClient = mock(AdminClient.class);
    ListConsumerGroupsResult groups = mock(ListConsumerGroupsResult.class);
    when(groups.all()).thenReturn(KafkaFuture.completedFuture(List.of(
        new ConsumerGroupListing("app.a", false), new ConsumerGroupListing("app.b", false))));
    when(adminClient.listConsumerGroups()).thenReturn(groups);
    ListConsumerGroupOffsetsResult offsetsA = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsA.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(Map.of(leaderless, new OffsetAndMetadata(10L))));
    when(adminClient.listConsumerGroupOffsets("app.a")).thenReturn(offsetsA);
    ListConsumerGroupOffsetsResult offsetsB = mock(ListConsumerGroupOffsetsResult.class);
    when(offsetsB.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(Map.of(healthy, new OffsetAndMetadata(10L))));
    when(adminClient.listConsumerGroupOffsets("app.b")).thenReturn(offsetsB);
    KafkaFutureImpl<ListOffsetsResultInfo> timedOut = new KafkaFutureImpl<>();
    timedOut.completeExceptionally(new TimeoutException("no leader"));
    when(adminClient.listOffsets(anyMap())).thenReturn(new ListOffsetsResult(Map.of(
        leaderless, timedOut,
        healthy, KafkaFuture.completedFuture(new ListOffsetsResultInfo(15L, -1L, Optional.empty())))));
    describes(adminClient,
        group("app.a", ConsumerGroupState.EMPTY), group("app.b", ConsumerGroupState.EMPTY));
    partitions(adminClient, Map.of("leaderless", 1, "healthy", 1));

    ExpiredRecords expired = RetentionMetrics.expiredRecords(adminClient, GROUPS, new HashMap<>());
    assertEquals(Map.of(new GroupTopic("app.b", "healthy"), 5L), expired.values());
    assertEquals(Map.of(), expired.failedGroups());
    assertEquals(Set.of(new GroupTopic("app.a", "leaderless")), expired.failedTopics().keySet());
    assertTrue(expired.failedTopics().get(new GroupTopic("app.a", "leaderless")).contains("no leader"));
  }

  @Test
  public void testExpiredRecordsCountOnlyTopicsLiveGroupsAreAssigned() {
    final TopicPartition current = new TopicPartition("current", 0);
    final TopicPartition previous = new TopicPartition("previous", 0);
    AdminClient adminClient = mock(AdminClient.class);
    ListConsumerGroupsResult groups = mock(ListConsumerGroupsResult.class);
    when(groups.all()).thenReturn(KafkaFuture.completedFuture(List.of(
        new ConsumerGroupListing("app.live", false),
        new ConsumerGroupListing("app.stopped", false),
        new ConsumerGroupListing("app.rebalancing", false))));
    when(adminClient.listConsumerGroups()).thenReturn(groups);
    ListConsumerGroupOffsetsResult offsets = mock(ListConsumerGroupOffsetsResult.class);
    when(offsets.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(Map.of(
        current, new OffsetAndMetadata(10L), previous, new OffsetAndMetadata(5L))));
    when(adminClient.listConsumerGroupOffsets(anyString())).thenReturn(offsets);
    when(adminClient.listOffsets(anyMap())).thenReturn(new ListOffsetsResult(Map.of(
        current, KafkaFuture.completedFuture(new ListOffsetsResultInfo(12L, -1L, Optional.empty())),
        previous, KafkaFuture.completedFuture(new ListOffsetsResultInfo(100L, -1L, Optional.empty())))));
    describes(adminClient,
        group("app.live", ConsumerGroupState.STABLE, current),
        group("app.stopped", ConsumerGroupState.EMPTY),
        group("app.rebalancing", ConsumerGroupState.PREPARING_REBALANCE, current));
    partitions(adminClient, Map.of("current", 1, "previous", 1));

    ExpiredRecords expired = RetentionMetrics.expiredRecords(adminClient, GROUPS, new HashMap<>());
    assertEquals(Map.of(
        new GroupTopic("app.live", "current"), 2L,
        new GroupTopic("app.stopped", "current"), 2L,
        new GroupTopic("app.stopped", "previous"), 95L), expired.values());
    assertEquals(Set.of("app.rebalancing"), expired.failedGroups().keySet());
  }

  @Test
  public void testUnreadExpiredCountsLogStartGrowthSinceFirstSeen() {
    TopicPartition p1 = new TopicPartition("t", 1);
    TopicPartition p2 = new TopicPartition("t", 2);
    Map<GroupPartition, Long> since = new HashMap<>();
    // first sight: records deleted before the enforcer saw the partition (here 500) do not count
    assertEquals(Map.of(new GroupTopic("app.a", "t"), 0L),
        RetentionMetrics.unreadExpired(Map.of("app.a", Set.of(p1, p2)), Map.of(p1, 0L, p2, 500L), since));
    assertEquals(Map.of(new GroupPartition("app.a", p1), 0L, new GroupPartition("app.a", p2), 500L), since);
    assertEquals(Map.of(new GroupTopic("app.a", "t"), 70L),
        RetentionMetrics.unreadExpired(Map.of("app.a", Set.of(p1, p2)), Map.of(p1, 40L, p2, 530L), since));
  }

  @Test
  public void testExpiredRecordsCountRecordsDeletedFromPartitionsTheGroupNeverRead() {
    final TopicPartition read = new TopicPartition("events", 0);
    final TopicPartition added = new TopicPartition("events", 1);
    final TopicPartition addedEmpty = new TopicPartition("events", 2);
    AdminClient adminClient = mock(AdminClient.class);
    ListConsumerGroupsResult groups = mock(ListConsumerGroupsResult.class);
    when(groups.all())
        .thenReturn(KafkaFuture.completedFuture(List.of(new ConsumerGroupListing("app.live", false))));
    when(adminClient.listConsumerGroups()).thenReturn(groups);
    ListConsumerGroupOffsetsResult offsets = mock(ListConsumerGroupOffsetsResult.class);
    when(offsets.partitionsToOffsetAndMetadata())
        .thenReturn(KafkaFuture.completedFuture(Map.of(read, new OffsetAndMetadata(10L))));
    when(adminClient.listConsumerGroupOffsets("app.live")).thenReturn(offsets);
    // partitions 1 and 2 were added to the topic and never assigned to the group
    describes(adminClient, group("app.live", ConsumerGroupState.STABLE, read));
    partitions(adminClient, Map.of("events", 3));
    Map<GroupPartition, Long> since = new HashMap<>();

    when(adminClient.listOffsets(anyMap())).thenReturn(logStarts(Map.of(read, 12L, added, 0L, addedEmpty, 5L)));
    assertEquals(Map.of(new GroupTopic("app.live", "events"), 2L),
        RetentionMetrics.expiredRecords(adminClient, GROUPS, since).values());

    // retention deleted 40 unread records from partition 1
    when(adminClient.listOffsets(anyMap())).thenReturn(logStarts(Map.of(read, 12L, added, 40L, addedEmpty, 5L)));
    assertEquals(Map.of(new GroupTopic("app.live", "events"), 42L),
        RetentionMetrics.expiredRecords(adminClient, GROUPS, since).values());

    // the group picks partition 1 up: it counts as committed again and its state is dropped
    when(offsets.partitionsToOffsetAndMetadata()).thenReturn(KafkaFuture.completedFuture(
        Map.of(read, new OffsetAndMetadata(10L), added, new OffsetAndMetadata(40L))));
    describes(adminClient, group("app.live", ConsumerGroupState.STABLE, read, added));
    assertEquals(Map.of(new GroupTopic("app.live", "events"), 2L),
        RetentionMetrics.expiredRecords(adminClient, GROUPS, since).values());
    assertEquals(Set.of(new GroupPartition("app.live", addedEmpty)), since.keySet());
  }

  @Test
  public void testUpdateExportsExpiredRecordsAndDropsRemovedGroups() {
    AtomicReference<Map<GroupTopic, Long>> expired = new AtomicReference<>(Map.of(
        new GroupTopic("app.expired_a", "t1"), 0L,
        new GroupTopic("app.expired_b", "t1"), 42L));
    RetentionMetrics metrics = metrics(Map::of, () -> new ExpiredRecords(expired.get(), Map.of(), Map.of()));

    metrics.update();
    assertEquals(0.0, expired("app.expired_a", "t1"), 0.0);
    assertEquals(42.0, expired("app.expired_b", "t1"), 0.0);

    expired.set(Map.of(new GroupTopic("app.expired_a", "t1"), 7L));
    metrics.update();
    assertEquals(7.0, expired("app.expired_a", "t1"), 0.0);
    assertNull(expired("app.expired_b", "t1"));
  }

  @Test
  public void testUpdateUpdatesOneMetricWhenTheOtherFails() {
    AtomicBoolean offsetsDown = new AtomicBoolean(false);
    AtomicReference<String> retentionMs = new AtomicReference<>("86400000");
    RetentionMetrics metrics = metrics(
        () -> Map.of("independent", Map.of("retention.ms", retentionMs.get())),
        () -> {
          if (offsetsDown.get()) {
            throw new IllegalStateException("coordinator down");
          }
          return new ExpiredRecords(
              Map.of(new GroupTopic("app.independent", "independent"), 5L), Map.of(), Map.of());
        });
    metrics.update();

    offsetsDown.set(true);
    retentionMs.set("3600000");
    metrics.update();
    assertEquals(3600.0, exported("independent"), 0.0);
    assertEquals(5.0, expired("app.independent", "independent"), 0.0);
  }

  @Test
  public void testUpdateKeepsLastExpiredRecordsOfFailedGroups() {
    GroupTopic failingT1 = new GroupTopic("app.failing", "t1");
    GroupTopic failingT2 = new GroupTopic("app.failing", "t2");
    GroupTopic healthy = new GroupTopic("app.healthy", "t1");
    AtomicReference<ExpiredRecords> expired = new AtomicReference<>(
        new ExpiredRecords(Map.of(failingT1, 3L, failingT2, 0L, healthy, 1L), Map.of(), Map.of()));
    RetentionMetrics metrics = metrics(Map::of, expired::get);
    metrics.update();

    expired.set(new ExpiredRecords(Map.of(healthy, 9L), Map.of("app.failing", "coordinator moving"), Map.of()));
    metrics.update();
    assertEquals(3.0, expired("app.failing", "t1"), 0.0);
    assertEquals(0.0, expired("app.failing", "t2"), 0.0);
    assertEquals(9.0, expired("app.healthy", "t1"), 0.0);

    expired.set(new ExpiredRecords(Map.of(failingT1, 4L, healthy, 9L), Map.of(), Map.of()));
    metrics.update();
    assertEquals(4.0, expired("app.failing", "t1"), 0.0);
    assertNull(expired("app.failing", "t2"));

    // a group that is gone, not failed, loses its series
    expired.set(new ExpiredRecords(Map.of(healthy, 9L), Map.of(), Map.of()));
    metrics.update();
    assertNull(expired("app.failing", "t1"));
    assertEquals(9.0, expired("app.healthy", "t1"), 0.0);
  }

  @Test
  public void testUpdateKeepsLastExpiredRecordsOfTopicsThatCannotBeRead() {
    GroupTopic readable = new GroupTopic("app.partly", "readable");
    GroupTopic unreadable = new GroupTopic("app.partly", "unreadable");
    AtomicReference<ExpiredRecords> expired =
        new AtomicReference<>(new ExpiredRecords(Map.of(readable, 1L, unreadable, 2L), Map.of(), Map.of()));
    RetentionMetrics metrics = metrics(Map::of, expired::get);
    metrics.update();

    expired.set(new ExpiredRecords(Map.of(readable, 7L), Map.of(), Map.of(unreadable, "no leader")));
    metrics.update();
    assertEquals(7.0, expired("app.partly", "readable"), 0.0);
    assertEquals(2.0, expired("app.partly", "unreadable"), 0.0);
  }

  @Test
  public void testGroupsWithoutCommittedOffsetsAreNotDescribed() {
    TopicPartition read = new TopicPartition("events", 0);
    AdminClient adminClient = mock(AdminClient.class);
    lists(adminClient, "app.reading", "app.never_committed");
    commits(adminClient, "app.reading", Map.of(read, 10L));
    commits(adminClient, "app.never_committed", Map.of());
    describes(adminClient,
        group("app.reading", ConsumerGroupState.EMPTY), group("app.never_committed", ConsumerGroupState.EMPTY));
    partitions(adminClient, Map.of("events", 1));
    when(adminClient.listOffsets(anyMap())).thenReturn(logStarts(Map.of(read, 12L)));

    ExpiredRecords expired = RetentionMetrics.expiredRecords(adminClient, GROUPS, new HashMap<>());
    assertEquals(Map.of(new GroupTopic("app.reading", "events"), 2L), expired.values());
    // describing a group costs a coordinator lookup and a request, and one that never committed has nothing to count
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<String>> described = ArgumentCaptor.forClass(Collection.class);
    verify(adminClient).describeConsumerGroups(described.capture());
    assertEquals(Set.of("app.reading"), Set.copyOf(described.getValue()));
  }
}
