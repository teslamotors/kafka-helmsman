/*
 * Copyright (c) 2026. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import io.prometheus.client.Gauge;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Exports the retention of every topic as the brokers report it, so alerts can compare consumer lag with retention.
 * {@link TopicEnforcer} updates it with its other stats; the enforce command creates it only with
 * {@code --retentionmetrics} in continuous mode.
 */
public class RetentionMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(RetentionMetrics.class);

  static final String RETENTION_MS = "retention.ms";
  static final String CLEANUP_POLICY = "cleanup.policy";

  // prometheus metrics should be static, see https://git.io/fj17x
  private static final Gauge topicRetention =
      Gauge.build()
          .name("kafka_topic_enforcer_topic_retention_seconds")
          .help("Retention of a topic as reported by the brokers. Topics with infinite retention (-1) or " +
              "compact-only cleanup never expire data by time and are not exported.")
          .labelNames("topic")
          .register();

  /**
   * The config of each topic, and the cause per topic whose config could not be read; such a topic maps to an empty
   * config.
   */
  record ActualConfigs(Map<String, Map<String, String>> configs, Map<String, String> failedTopics) {
  }

  private final Supplier<ActualConfigs> actualConfigs;
  private Set<String> exportedTopics = Set.of();

  /**
   * Creates the metric, reading topic configs through the given admin client.
   *
   * @param adminClient reads topic configs
   */
  public RetentionMetrics(AdminClient adminClient) {
    this(() -> actualConfigs(adminClient));
  }

  // for testing
  RetentionMetrics(Supplier<ActualConfigs> actualConfigs) {
    this.actualConfigs = actualConfigs;
  }

  /**
   * Update the metric. Stats run before each enforcement, so this never throws: if the topic configs cannot be read,
   * the metric keeps its last values.
   */
  public void update() {
    try {
      ActualConfigs actual = actualConfigs.get();
      Map<String, Double> retention = retentionSeconds(actual.configs());
      retention.forEach((topic, seconds) -> topicRetention.labels(topic).set(seconds));
      // topics whose config could not be read keep their last value, the series of other topics that are gone or no
      // longer expire by time are removed
      Set<String> exported = new HashSet<>(retention.keySet());
      for (String topic : exportedTopics) {
        if (actual.failedTopics().containsKey(topic)) {
          exported.add(topic);
        } else if (!retention.containsKey(topic)) {
          topicRetention.remove(topic);
        }
      }
      exportedTopics = exported;
      logFailures(actual.failedTopics());
    } catch (RuntimeException e) {
      LOG.warn("Could not update the topic retention metric", e);
    }
  }

  // once per run at most (stats runs once per enforcement, every 600 s by default)
  private static void logFailures(Map<String, String> failedTopics) {
    if (!failedTopics.isEmpty()) {
      String sample = failedTopics.entrySet()
          .stream()
          .limit(10)
          .map(e -> e.getKey() + " (" + e.getValue() + ")")
          .collect(Collectors.joining(", "));
      LOG.warn("{} topics whose config could not be read, kept their last retention, up to 10 shown: {}",
          failedTopics.size(), sample);
    }
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
}
