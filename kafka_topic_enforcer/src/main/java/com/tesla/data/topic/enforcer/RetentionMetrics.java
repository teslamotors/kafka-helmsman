/*
 * Copyright (c) 2026. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import io.prometheus.client.Gauge;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Exports the retention of every topic and the records consumer groups lost to it, so alerts can compare consumer lag
 * with retention and see data that expired before it was read. {@link TopicEnforcer} updates it with its other stats;
 * the enforce command creates it only when the enforcer config has a {@code retentionMetrics} section, see
 * {@link Options#from(Map)}.
 */
public class RetentionMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(RetentionMetrics.class);

  static final String RETENTION_MS = "retention.ms";
  static final String CLEANUP_POLICY = "cleanup.policy";
  static final String CONFIG_KEY = "retentionMetrics";

  // prometheus metrics should be static, see https://git.io/fj17x
  private static final Gauge topicRetention =
      Gauge.build()
          .name("kafka_topic_enforcer_topic_retention_seconds")
          .help("Retention of a topic as reported by the brokers. Topics with infinite retention (-1) or " +
              "compact-only cleanup never expire data by time and are not exported.")
          .labelNames("topic")
          .register();

  private static final Gauge consumerExpiredRecords =
      Gauge.build()
          .name("kafka_topic_enforcer_consumer_expired_records")
          .help("Records a consumer group had not read when Kafka deleted them, sampled before each enforcement run.")
          .labelNames("consumer", "topic")
          .register();

  /**
   * The {@code retentionMetrics} config section.
   *
   * @param consumerGroups the consumer groups whose expired records are exported (full match)
   */
  public record Options(Pattern consumerGroups) {

    /**
     * Reads the optional {@code retentionMetrics} section of the enforcer config: {@code consumerGroups}, a regular
     * expression (default: all groups). An empty section ({@code retentionMetrics: {}}) turns the metrics on with the
     * defaults.
     */
    public static Options from(Map<String, Object> config) {
      Object section = config.get(CONFIG_KEY);
      if (section == null) {
        section = Map.of();
      }
      if (!(section instanceof Map<?, ?> values)) {
        throw new IllegalArgumentException(CONFIG_KEY + " must be a map, got " + section);
      }
      Object groups = values.get("consumerGroups");
      if (groups != null && !(groups instanceof String)) {
        throw new IllegalArgumentException(CONFIG_KEY + ".consumerGroups must be a regular expression, got " + groups);
      }
      return new Options(Pattern.compile(groups != null ? (String) groups : ".*"));
    }
  }

  /** A consumer group and one of the topics it committed offsets for. */
  record GroupTopic(String group, String topic) {
  }

  /** A consumer group and a partition it has never committed, see {@link #unreadExpired(Map, Map, Map)}. */
  record GroupPartition(String group, TopicPartition partition) {
  }

  /**
   * Expired records per group and topic; the cause per group whose committed offsets or assignment could not be read;
   * and the cause per group and topic whose topic could not be read, while the group's other topics could.
   */
  record ExpiredRecords(
      Map<GroupTopic, Long> values, Map<String, String> failedGroups, Map<GroupTopic, String> failedTopics) {
  }

  /**
   * The config of each topic, and the cause per topic whose config could not be read; such a topic maps to an empty
   * config.
   */
  record ActualConfigs(Map<String, Map<String, String>> configs, Map<String, String> failedTopics) {
  }

  private final Supplier<ActualConfigs> actualConfigs;
  private final Supplier<ExpiredRecords> expiredRecords;
  // partitions each group has never committed and their log start offset when first seen, see unreadExpired
  private final Map<GroupPartition, Long> unreadSince = new HashMap<>();
  private Set<String> exportedRetention = Set.of();
  private Set<GroupTopic> exportedExpiredRecords = Set.of();

  /**
   * The constructor.
   *
   * @param adminClient reads topic configs, consumer group offsets and log start offsets
   * @param options the {@code retentionMetrics} config section
   */
  public RetentionMetrics(AdminClient adminClient, Options options) {
    this.actualConfigs = () -> actualConfigs(adminClient);
    this.expiredRecords = () -> expiredRecords(adminClient, options.consumerGroups(), unreadSince);
  }

  /**
   * The retention metrics the enforce command should update, or null if they are off.
   *
   * @param adminClient reads topic configs, consumer group offsets and log start offsets
   * @param config the enforcer config, see {@link Options#from(Map)}
   * @param continuous whether the enforcer runs continuously; the metrics are on only then, and only if the config
   *     has a {@code retentionMetrics} section
   */
  static RetentionMetrics fromConfig(AdminClient adminClient, Map<String, Object> config, boolean continuous) {
    // a one-shot run has no metrics server to export them
    return continuous && config.containsKey(CONFIG_KEY)
        ? new RetentionMetrics(adminClient, Options.from(config))
        : null;
  }

  // for testing
  RetentionMetrics(Supplier<ActualConfigs> actualConfigs, Supplier<ExpiredRecords> expiredRecords) {
    this.actualConfigs = actualConfigs;
    this.expiredRecords = expiredRecords;
  }

  /**
   * Update both metrics. Stats run before each enforcement, so this never throws: a metric that cannot be updated
   * keeps its last values.
   */
  public void update() {
    try {
      ActualConfigs actual = actualConfigs.get();
      Map<String, Double> retention = retentionSeconds(actual.configs());
      // topics whose config could not be read keep their last values
      Set<String> kept = exportedRetention.stream()
          .filter(topic -> actual.failedTopics().containsKey(topic))
          .collect(Collectors.toSet());
      Set<String> previous = new HashSet<>(exportedRetention);
      previous.removeAll(kept);
      Set<String> exported = new HashSet<>(export(topicRetention, retention, previous,
          topic -> new String[] {topic}));
      exported.addAll(kept);
      exportedRetention = exported;
      logFailures("topics whose config could not be read, kept their last retention",
          actual.failedTopics());
    } catch (RuntimeException e) {
      LOG.warn("Could not update the topic retention metric", e);
    }
    try {
      ExpiredRecords expired = expiredRecords.get();
      // groups and topics that could not be read keep their last values
      Set<GroupTopic> kept = exportedExpiredRecords.stream()
          .filter(key -> expired.failedGroups().containsKey(key.group()) || expired.failedTopics().containsKey(key))
          .collect(Collectors.toSet());
      Set<GroupTopic> previous = new HashSet<>(exportedExpiredRecords);
      previous.removeAll(kept);
      Set<GroupTopic> exported = new HashSet<>(export(consumerExpiredRecords, expired.values(), previous,
          key -> new String[] {key.group(), key.topic()}));
      exported.addAll(kept);
      exportedExpiredRecords = exported;
      logFailures(
          "consumer groups whose offsets or assignment could not be read, kept their last expired records",
          expired.failedGroups());
      logFailures(
          "consumer group topics that could not be read, kept their last expired records",
          expired.failedTopics().entrySet().stream().collect(Collectors.toMap(
              e -> e.getKey().group() + "/" + e.getKey().topic(), Map.Entry::getValue)));
    } catch (RuntimeException e) {
      LOG.warn("Could not update the consumer group expired records metric", e);
    }
  }

  // once per run at most (stats runs once per enforcement, every 600 s by default)
  private static void logFailures(String what, Map<String, String> failed) {
    if (!failed.isEmpty()) {
      String sample = failed.entrySet()
          .stream()
          .limit(10)
          .map(e -> e.getKey() + " (" + e.getValue() + ")")
          .collect(Collectors.joining(", "));
      LOG.warn("{} {}, up to 10 shown: {}", failed.size(), what, sample);
    }
  }

  // sets the gauge to the given values and removes the series of keys that are gone, returns the exported keys
  private static <K> Set<K> export(
      Gauge gauge, Map<K, ? extends Number> values, Set<K> exported, Function<K, String[]> labels) {
    values.forEach((key, value) -> gauge.labels(labels.apply(key)).set(value.doubleValue()));
    exported.stream()
        .filter(key -> !values.containsKey(key))
        .forEach(key -> gauge.remove(labels.apply(key)));
    return values.keySet();
  }

  /**
   * Retention in seconds of each topic, from its config as the brokers report it. Topics with infinite retention or
   * compact-only cleanup are left out.
   */
  static Map<String, Double> retentionSeconds(Map<String, Map<String, String>> actual) {
    Map<String, Double> retention = new HashMap<>();
    actual.forEach((topic, config) -> {
      String retentionMs = config.get(RETENTION_MS);
      String cleanupPolicy = config.getOrDefault(CLEANUP_POLICY, "delete");
      if (retentionMs != null && cleanupPolicy.contains("delete") && Long.parseLong(retentionMs) >= 0) {
        retention.put(topic, Long.parseLong(retentionMs) / 1000.0);
      }
    });
    return retention;
  }

  /**
   * The config of every non-internal topic as the brokers report it, broker defaults included. A topic whose config
   * could not be read maps to an empty config and is reported with the cause.
   */
  static ActualConfigs actualConfigs(AdminClient adminClient) {
    try {
      List<ConfigResource> resources = adminClient.listTopics(new ListTopicsOptions().listInternal(false))
          .names()
          .get()
          .stream()
          .filter(topic -> !topic.startsWith("_"))
          .map(topic -> new ConfigResource(ConfigResource.Type.TOPIC, topic))
          .collect(Collectors.toList());
      Map<ConfigResource, KafkaFuture<Config>> futures = adminClient.describeConfigs(resources).values();
      Map<String, Map<String, String>> configs = new HashMap<>();
      Map<String, String> failedTopics = new HashMap<>();
      for (Map.Entry<ConfigResource, KafkaFuture<Config>> e : futures.entrySet()) {
        String topic = e.getKey().name();
        try {
          configs.put(topic, entries(e.getValue().get()));
        } catch (ExecutionException ex) {
          configs.put(topic, Map.of());
          failedTopics.put(topic, String.valueOf(ex.getCause()));
        }
      }
      return new ActualConfigs(configs, failedTopics);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private static Map<String, String> entries(Config config) {
    return config.entries()
        .stream()
        .filter(e -> e.value() != null)
        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value));
  }

  /**
   * Records each consumer group matching groups had not read when Kafka deleted them, per topic: on committed
   * partitions see {@link #expiredRecords(Map, Map)}, on partitions of the same topics the group never committed see
   * {@link #unreadExpired(Map, Map, Map)} (unreadSince holds their state between calls). Only the topics a group
   * currently reads count, see {@link #currentTopicsOnly(AdminClient, Map, Map)}. A group whose committed offsets or
   * assignment cannot be read, or that reads a topic whose partitions or log start offsets cannot be read, is reported
   * as failed; the whole call fails only if the groups cannot be listed.
   */
  static ExpiredRecords expiredRecords(
      AdminClient adminClient, Pattern groups, Map<GroupPartition, Long> unreadSince) {
    try {
      Map<String, String> failedGroups = new HashMap<>();
      Map<String, Map<TopicPartition, Long>> committed =
          currentTopicsOnly(adminClient, committedOffsets(adminClient, groups, failedGroups), failedGroups);
      Set<String> topics = committed.values()
          .stream()
          .flatMap(offsets -> topicsOf(offsets).stream())
          .collect(Collectors.toSet());
      Map<String, String> failedTopics = new HashMap<>();
      Map<String, Integer> partitionCount = partitionCounts(adminClient, topics, failedTopics);
      Map<TopicPartition, Long> logStart = logStartOffsets(adminClient, partitionCount, failedTopics);
      // a topic that could not be read keeps its last values for the groups reading it; their other topics update
      Map<GroupTopic, String> failedGroupTopics = new HashMap<>();
      Map<String, Map<TopicPartition, Long>> readable = new HashMap<>();
      Map<String, Set<TopicPartition>> readableUnread = new HashMap<>();
      committed.forEach((group, offsets) -> {
        if (failedGroups.containsKey(group)) {
          return;
        }
        Map<TopicPartition, Long> readableOffsets = new HashMap<>(offsets);
        for (String topic : topicsOf(offsets)) {
          if (failedTopics.containsKey(topic)) {
            failedGroupTopics.put(new GroupTopic(group, topic), failedTopics.get(topic));
            readableOffsets.keySet().removeIf(partition -> partition.topic().equals(topic));
          }
        }
        readable.put(group, readableOffsets);
        readableUnread.put(group, unreadPartitions(readableOffsets, partitionCount));
      });
      // failed groups and topics keep their state, like their exported values; groups and topics that are gone drop it
      unreadSince.keySet().removeIf(key -> !failedGroups.containsKey(key.group()) &&
          !failedGroupTopics.containsKey(new GroupTopic(key.group(), key.partition().topic())) &&
          !readableUnread.getOrDefault(key.group(), Set.of()).contains(key.partition()));
      Map<GroupTopic, Long> values = new HashMap<>(expiredRecords(readable, logStart));
      unreadExpired(readableUnread, logStart, unreadSince).forEach((key, value) -> values.merge(key, value, Long::sum));
      return new ExpiredRecords(values, failedGroups, failedGroupTopics);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Per group and topic, the sum over the partitions of max(0, log start offset - committed offset). Partitions whose
   * log start offset is unknown are left out.
   */
  static Map<GroupTopic, Long> expiredRecords(
      Map<String, Map<TopicPartition, Long>> committed, Map<TopicPartition, Long> logStart) {
    Map<GroupTopic, Long> expired = new HashMap<>();
    committed.forEach((group, offsets) -> offsets.forEach((partition, offset) -> {
      Long start = logStart.get(partition);
      if (start != null) {
        expired.merge(new GroupTopic(group, partition.topic()), Math.max(0, start - offset), Long::sum);
      }
    }));
    return expired;
  }

  /**
   * Per group and topic, the records deleted from partitions the group never committed while they were unread: the
   * growth of their log start offset since unreadSince first recorded it. A partition seen for the first time is
   * recorded at its current log start and counts 0, so records Kafka deleted before the enforcer saw the partition
   * (for example before the group existed, or before an enforcer restart) are not counted. Partitions whose log start
   * offset is unknown are left out.
   */
  static Map<GroupTopic, Long> unreadExpired(
      Map<String, Set<TopicPartition>> unread, Map<TopicPartition, Long> logStart,
      Map<GroupPartition, Long> unreadSince) {
    Map<GroupTopic, Long> expired = new HashMap<>();
    unread.forEach((group, partitions) -> partitions.forEach(partition -> {
      Long start = logStart.get(partition);
      if (start != null) {
        long since = unreadSince.computeIfAbsent(new GroupPartition(group, partition), key -> start);
        expired.merge(new GroupTopic(group, partition.topic()), Math.max(0, start - since), Long::sum);
      }
    }));
    return expired;
  }

  private static Set<String> topicsOf(Map<TopicPartition, Long> offsets) {
    return offsets.keySet().stream().map(TopicPartition::topic).collect(Collectors.toSet());
  }

  // The partitions of the topics a group reads (the topics of its committed offsets) that it has never committed:
  // partitions it was never assigned, such as ones added to the topic later. Deleted topics have none.
  private static Set<TopicPartition> unreadPartitions(
      Map<TopicPartition, Long> offsets, Map<String, Integer> partitionCount) {
    Set<TopicPartition> unread = new HashSet<>();
    for (String topic : topicsOf(offsets)) {
      for (int p = 0; p < partitionCount.getOrDefault(topic, 0); p++) {
        TopicPartition partition = new TopicPartition(topic, p);
        if (!offsets.containsKey(partition)) {
          unread.add(partition);
        }
      }
    }
    return unread;
  }

  // The partition count of each topic. Deleted topics are left out. A topic that cannot be described, or that has a
  // partition without a leader, goes to failedTopics: kafka-clients 2.8 fails every partition of a listOffsets call
  // when any topic in it has a partition without a leader.
  private static Map<String, Integer> partitionCounts(
      AdminClient adminClient, Set<String> topics, Map<String, String> failedTopics) throws InterruptedException {
    if (topics.isEmpty()) {
      return Map.of();
    }
    Map<String, KafkaFuture<TopicDescription>> described = adminClient.describeTopics(topics).values();
    Map<String, Integer> partitionCount = new HashMap<>();
    for (String topic : topics) {
      try {
        List<TopicPartitionInfo> partitions = described.get(topic).get().partitions();
        Optional<TopicPartitionInfo> leaderless = partitions.stream().filter(p -> p.leader() == null).findFirst();
        if (leaderless.isPresent()) {
          failedTopics.put(topic, "has no leader for partition " + leaderless.get().partition());
        } else {
          partitionCount.put(topic, partitions.size());
        }
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          failedTopics.put(topic, "could not be described: " + e.getCause());
        }
      }
    }
    return partitionCount;
  }

  // The committed offsets of each consumer group matching groups, partitions without a committed offset are left out,
  // and so are groups that have none: they have nothing to count. Groups whose offsets cannot be read go to
  // failedGroups. This client version fetches one group per request, so all
  // requests are sent before waiting on any.
  private static Map<String, Map<TopicPartition, Long>> committedOffsets(
      AdminClient adminClient, Pattern groups, Map<String, String> failedGroups)
      throws InterruptedException, ExecutionException {
    Map<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> futures = new HashMap<>();
    for (ConsumerGroupListing group : adminClient.listConsumerGroups().all().get()) {
      if (groups.matcher(group.groupId()).matches()) {
        futures.put(group.groupId(), adminClient.listConsumerGroupOffsets(group.groupId())
            .partitionsToOffsetAndMetadata());
      }
    }
    Map<String, Map<TopicPartition, Long>> committed = new HashMap<>();
    for (Map.Entry<String, KafkaFuture<Map<TopicPartition, OffsetAndMetadata>>> e : futures.entrySet()) {
      try {
        Map<TopicPartition, Long> offsets = new HashMap<>();
        e.getValue().get().forEach((partition, offset) -> {
          if (offset != null) {
            offsets.put(partition, offset.offset());
          }
        });
        if (!offsets.isEmpty()) {
          committed.put(e.getKey(), offsets);
        }
      } catch (ExecutionException ex) {
        failedGroups.put(e.getKey(), String.valueOf(ex.getCause()));
      }
    }
    return committed;
  }

  /**
   * Kafka keeps a group's offsets for topics it stopped reading (offsets.retention.minutes, 7 days by default); their
   * log start keeps moving, so they would read as loss that never happened. A group with members counts only the
   * topics they are assigned; a group without members (stopped or stalled) counts every topic it committed. Groups
   * that are rebalancing or cannot be described go to failedGroups.
   */
  private static Map<String, Map<TopicPartition, Long>> currentTopicsOnly(
      AdminClient adminClient, Map<String, Map<TopicPartition, Long>> committed, Map<String, String> failedGroups)
      throws InterruptedException {
    if (committed.isEmpty()) {
      return committed;
    }
    Map<String, KafkaFuture<ConsumerGroupDescription>> described =
        adminClient.describeConsumerGroups(committed.keySet()).describedGroups();
    Map<String, Map<TopicPartition, Long>> current = new HashMap<>();
    for (Map.Entry<String, Map<TopicPartition, Long>> e : committed.entrySet()) {
      String group = e.getKey();
      try {
        ConsumerGroupDescription description = described.get(group).get();
        if (description.members().isEmpty()) {
          current.put(group, e.getValue());
        } else if (description.state() != ConsumerGroupState.STABLE) {
          failedGroups.put(group, "assignment unknown, group is " + description.state());
        } else {
          Set<String> topics = description.members()
              .stream()
              .flatMap(member -> member.assignment().topicPartitions().stream())
              .map(TopicPartition::topic)
              .collect(Collectors.toSet());
          Map<TopicPartition, Long> assigned = new HashMap<>(e.getValue());
          assigned.keySet().removeIf(partition -> !topics.contains(partition.topic()));
          current.put(group, assigned);
        }
      } catch (ExecutionException ex) {
        failedGroups.put(group, String.valueOf(ex.getCause()));
      }
    }
    return current;
  }

  // The log start offset of every partition of the given topics, with one listOffsets call per topic, all sent before
  // waiting on any: kafka-clients 2.8 fails a whole call, after its API timeout, when the metadata of any topic in it
  // has an error, so a topic deleted or left without a leader meanwhile only fails its own call. Topics deleted
  // meanwhile are left out; a topic whose offsets cannot be read goes to failedTopics.
  private static Map<TopicPartition, Long> logStartOffsets(
      AdminClient adminClient, Map<String, Integer> partitionCount, Map<String, String> failedTopics)
      throws InterruptedException {
    Map<String, ListOffsetsResult> results = new HashMap<>();
    partitionCount.forEach((topic, count) -> {
      Map<TopicPartition, OffsetSpec> request = new HashMap<>();
      for (int p = 0; p < count; p++) {
        request.put(new TopicPartition(topic, p), OffsetSpec.earliest());
      }
      results.put(topic, adminClient.listOffsets(request));
    });
    Map<TopicPartition, Long> logStart = new HashMap<>();
    for (Map.Entry<String, ListOffsetsResult> e : results.entrySet()) {
      String topic = e.getKey();
      Map<TopicPartition, Long> offsets = new HashMap<>();
      try {
        for (int p = 0; p < partitionCount.get(topic); p++) {
          TopicPartition partition = new TopicPartition(topic, p);
          offsets.put(partition, e.getValue().partitionResult(partition).get().offset());
        }
        logStart.putAll(offsets);
      } catch (ExecutionException ex) {
        if (!(ex.getCause() instanceof UnknownTopicOrPartitionException)) {
          failedTopics.put(topic, "log start offsets could not be read: " + ex.getCause());
        }
      }
    }
    return logStart;
  }
}
