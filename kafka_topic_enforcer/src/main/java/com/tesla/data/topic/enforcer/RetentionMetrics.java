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
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
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
import java.util.stream.Stream;

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
          .help("Records a consumer group had not read when Kafka deleted them, sampled once per " +
              "enforcement run: log start offset minus committed offset, summed over the partitions of the topic " +
              "the group committed; on a partition of that topic the group never committed (for example one added " +
              "later and never assigned), the log start offset's growth since the enforcer first saw it unread. " +
              "0 if none. Catches stalled and stopped groups; a live consumer that resets to the log start between " +
              "two samples is not counted.")
          .labelNames("consumer", "topic")
          .register();

  /**
   * The {@code retentionMetrics} config section.
   *
   * @param consumerGroups the consumer groups whose expired records are exported (full match)
   */
  record Options(Pattern consumerGroups) {

    /** Whether the enforcer config turns the retention metrics on, that is, has a {@code retentionMetrics} section. */
    static boolean enabled(Map<String, Object> config) {
      return config.containsKey(CONFIG_KEY);
    }

    /**
     * Reads the optional {@code retentionMetrics} section of the enforcer config: {@code consumerGroups}, a regular
     * expression (default: all groups). An empty section ({@code retentionMetrics: {}}) turns the metrics on with the
     * defaults.
     */
    static Options from(Map<String, Object> config) {
      Object section = config.get(CONFIG_KEY);
      if (section == null) {
        section = Map.of();
      }
      if (!(section instanceof Map<?, ?> values)) {
        throw new IllegalArgumentException(CONFIG_KEY + " must be a map, got " + section);
      }
      Object groups = values.get("consumerGroups");
      return new Options(Pattern.compile(groups != null ? String.valueOf(groups) : ".*"));
    }
  }

  /** A consumer group and one of the topics it committed offsets for. */
  record GroupTopic(String group, String topic) {
  }

  /** A consumer group and a partition it has never committed, see {@link #unreadExpired(Map, Map, Map)}. */
  record GroupPartition(String group, TopicPartition partition) {
  }

  /** Expired records per group and topic, and the cause per group whose committed offsets could not be read. */
  record ExpiredRecords(Map<GroupTopic, Long> values, Map<String, String> failedGroups) {
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

  // for testing
  RetentionMetrics(Supplier<ActualConfigs> actualConfigs, Supplier<ExpiredRecords> expiredRecords) {
    this.actualConfigs = actualConfigs;
    this.expiredRecords = expiredRecords;
  }

  /**
   * Update both metrics. Stats run before each enforcement, so this never throws: a metric that cannot be updated
   * keeps its last values.
   *
   * @param configured the configured topics, whose desired retention is used when a topic's config cannot be read
   */
  public void update(Collection<ConfiguredTopic> configured) {
    try {
      ActualConfigs actual = actualConfigs.get();
      Map<String, Double> retention = retentionSeconds(actual.configs(), configured);
      // topics whose config could not be read and that have no desired retention keep their last values
      Set<String> kept = exportedRetention.stream()
          .filter(topic -> actual.failedTopics().containsKey(topic) && !retention.containsKey(topic))
          .collect(Collectors.toSet());
      Set<String> previous = new HashSet<>(exportedRetention);
      previous.removeAll(kept);
      Set<String> exported = new HashSet<>(export(topicRetention, retention, previous,
          topic -> new String[] {topic}));
      exported.addAll(kept);
      exportedRetention = exported;
      logFailures("topics whose config could not be read, using their desired or last retention",
          actual.failedTopics());
    } catch (RuntimeException e) {
      LOG.warn("Could not update the topic retention metric", e);
    }
    try {
      ExpiredRecords expired = expiredRecords.get();
      // groups whose offsets could not be read keep their last values
      Set<GroupTopic> kept = exportedExpiredRecords.stream()
          .filter(key -> expired.failedGroups().containsKey(key.group()))
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
   * Retention in seconds of each topic on the cluster, from its actual config, or from its desired config when the
   * actual one could not be read. Topics with infinite retention or compact-only cleanup are left out.
   */
  static Map<String, Double> retentionSeconds(
      Map<String, Map<String, String>> actual, Collection<ConfiguredTopic> desired) {
    Map<String, Map<String, String>> desiredConfigs = desired.stream()
        .collect(Collectors.toMap(ConfiguredTopic::getName, ConfiguredTopic::getConfig));
    Map<String, Double> retention = new HashMap<>();
    actual.forEach((topic, config) -> {
      Map<String, String> fallback = desiredConfigs.getOrDefault(topic, Map.of());
      String retentionMs = config.getOrDefault(RETENTION_MS, fallback.get(RETENTION_MS));
      String cleanupPolicy = config.getOrDefault(CLEANUP_POLICY, fallback.getOrDefault(CLEANUP_POLICY, "delete"));
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
   * currently reads count, see {@link #currentTopicsOnly(AdminClient, Map, Map)}. A group whose committed offsets,
   * assignment, topic partitions or log start offsets cannot be read is reported as failed; the whole call fails only
   * if the groups cannot be listed.
   */
  static ExpiredRecords expiredRecords(
      AdminClient adminClient, Pattern groups, Map<GroupPartition, Long> unreadSince) {
    try {
      Map<String, String> failedGroups = new HashMap<>();
      Map<String, Map<TopicPartition, Long>> committed =
          currentTopicsOnly(adminClient, committedOffsets(adminClient, groups, failedGroups), failedGroups);
      Map<String, Set<TopicPartition>> unread = unreadPartitions(adminClient, committed, failedGroups);
      Set<TopicPartition> partitions = new HashSet<>();
      committed.values().forEach(offsets -> partitions.addAll(offsets.keySet()));
      unread.values().forEach(partitions::addAll);
      Map<TopicPartition, String> failedPartitions = new HashMap<>();
      Map<TopicPartition, Long> logStart = logStartOffsets(adminClient, partitions, failedPartitions);
      // a group with a partition whose log start could not be read keeps its last values; the others still update
      Map<String, Map<TopicPartition, Long>> readable = new HashMap<>();
      Map<String, Set<TopicPartition>> readableUnread = new HashMap<>();
      committed.forEach((group, offsets) -> {
        if (failedGroups.containsKey(group)) {
          return;
        }
        Set<TopicPartition> groupUnread = unread.getOrDefault(group, Set.of());
        Optional<TopicPartition> failed = Stream.concat(offsets.keySet().stream(), groupUnread.stream())
            .filter(failedPartitions::containsKey)
            .findFirst();
        if (failed.isPresent()) {
          failedGroups.put(group,
              "log start offset of " + failed.get() + " could not be read: " + failedPartitions.get(failed.get()));
        } else {
          readable.put(group, offsets);
          readableUnread.put(group, groupUnread);
        }
      });
      // failed groups keep their state, like their exported values; groups that are gone drop theirs
      unreadSince.keySet().removeIf(key -> !failedGroups.containsKey(key.group()) &&
          !readableUnread.getOrDefault(key.group(), Set.of()).contains(key.partition()));
      Map<GroupTopic, Long> values = new HashMap<>(expiredRecords(readable, logStart));
      unreadExpired(readableUnread, logStart, unreadSince).forEach((key, value) -> values.merge(key, value, Long::sum));
      return new ExpiredRecords(values, failedGroups);
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

  // The partitions of the topics each group reads (the topics of its committed offsets) that it has never committed:
  // partitions it was never assigned, such as ones added to the topic later. Deleted topics have none; a group
  // reading a topic that cannot be described goes to failedGroups.
  private static Map<String, Set<TopicPartition>> unreadPartitions(
      AdminClient adminClient, Map<String, Map<TopicPartition, Long>> committed, Map<String, String> failedGroups)
      throws InterruptedException {
    Set<String> topics = committed.values()
        .stream()
        .flatMap(offsets -> offsets.keySet().stream())
        .map(TopicPartition::topic)
        .collect(Collectors.toSet());
    if (topics.isEmpty()) {
      return Map.of();
    }
    Map<String, KafkaFuture<TopicDescription>> described = adminClient.describeTopics(topics).values();
    Map<String, Integer> partitionCount = new HashMap<>();
    Map<String, String> failedTopics = new HashMap<>();
    for (String topic : topics) {
      try {
        partitionCount.put(topic, described.get(topic).get().partitions().size());
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          failedTopics.put(topic, String.valueOf(e.getCause()));
        }
      }
    }
    Map<String, Set<TopicPartition>> unread = new HashMap<>();
    committed.forEach((group, offsets) -> {
      Set<String> groupTopics = offsets.keySet().stream().map(TopicPartition::topic).collect(Collectors.toSet());
      Optional<String> failed = groupTopics.stream().filter(failedTopics::containsKey).findFirst();
      if (failed.isPresent()) {
        failedGroups.put(group,
            "topic " + failed.get() + " could not be described: " + failedTopics.get(failed.get()));
        return;
      }
      Set<TopicPartition> groupUnread = new HashSet<>();
      for (String topic : groupTopics) {
        for (int p = 0; p < partitionCount.getOrDefault(topic, 0); p++) {
          TopicPartition partition = new TopicPartition(topic, p);
          if (!offsets.containsKey(partition)) {
            groupUnread.add(partition);
          }
        }
      }
      unread.put(group, groupUnread);
    });
    return unread;
  }

  // The committed offsets of each consumer group matching groups, partitions without a committed offset are left out;
  // groups whose offsets cannot be read go to failedGroups. This client version fetches one group per request, so all
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
        committed.put(e.getKey(), offsets);
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

  // One request per partition leader. Partitions of deleted topics that groups still have offsets for are left out;
  // partitions that fail otherwise go to failedPartitions with the cause.
  private static Map<TopicPartition, Long> logStartOffsets(
      AdminClient adminClient, Set<TopicPartition> partitions, Map<TopicPartition, String> failedPartitions)
      throws InterruptedException {
    if (partitions.isEmpty()) {
      return Map.of();
    }
    ListOffsetsResult result = adminClient.listOffsets(
        partitions.stream().collect(Collectors.toMap(Function.identity(), partition -> OffsetSpec.earliest())));
    Map<TopicPartition, Long> logStart = new HashMap<>();
    for (TopicPartition partition : partitions) {
      try {
        logStart.put(partition, result.partitionResult(partition).get().offset());
      } catch (ExecutionException e) {
        if (!(e.getCause() instanceof UnknownTopicOrPartitionException)) {
          failedPartitions.put(partition, String.valueOf(e.getCause()));
        }
      }
    }
    return logStart;
  }
}
