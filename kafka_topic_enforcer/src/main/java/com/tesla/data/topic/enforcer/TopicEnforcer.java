/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static tesla.shade.com.google.common.base.Preconditions.checkArgument;

import com.tesla.data.enforcer.Enforcer;
import com.tesla.data.topic.enforcer.ConfigDrift.Result;
import com.tesla.data.topic.enforcer.ConfigDrift.Type;

import io.prometheus.client.Gauge;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Enforce configured topics state on a kafka cluster. */
public class TopicEnforcer extends Enforcer<ConfiguredTopic> {

  private final ConfigDrift configDrift;
  private final Set<Result> configDriftSafetyFilters;
  private final TopicService topicService;
  // prometheus metrics should be static, see https://git.io/fj17x
  private static final Gauge partitionCountDrift =
      Gauge.build()
          .name("kafka_topic_enforcer_partition_count_drift_topics")
          .help("Count of topics which have drifted from their desired partition count.")
          .labelNames("type")
          .register();

  private static final Gauge topicConfigDrift =
      Gauge.build()
          .name("kafka_topic_enforcer_topic_config_drift_topics")
          .help("Count of topics which have drifted from their desired topic config.")
          .labelNames("type")
          .register();

  private static final Gauge unsupportedDrift =
      Gauge.build()
          .name("kafka_topic_enforcer_unsupported_drift_topics")
          .help("Count of topics having config drifts which aren't supported for enforcement.")
          .labelNames("type")
          .register();

  public TopicEnforcer(
      TopicService topicService,
      List<ConfiguredTopic> configuredTopics,
      ConfigDrift configDrift,
      boolean safemode) {
    super(
        configuredTopics,
        () -> topicService.listExisting(true).values(),
        (t1, t2) -> t1.getName().equals(t2.getName()),
        safemode);
    checkArgument(
        configuredTopics.stream().noneMatch(TopicService.INTERNAL_TOPIC),
        "Internal topics found in config");
    this.topicService = topicService;
    this.configDrift = configDrift;
    this.configDriftSafetyFilters =
        safemode
            ? EnumSet.of(Result.SAFE_DRIFT)
            : EnumSet.of(Result.SAFE_DRIFT, Result.UNSAFE_DRIFT);
  }

  public TopicEnforcer(
      TopicService topicService, List<ConfiguredTopic> configuredTopics, boolean safemode) {
    this(topicService, configuredTopics, new ConfigDrift(), safemode);
  }

  /**
   * Find topics whose actual configuration has drifted from expected configuration. The returned
   * list will have topic represented in the 'configured' aka 'desired' form.
   *
   * @param type the mode in which to perform config drift check, see {@link ConfigDrift.Type}
   * @param safetyFilters a set of filters to apply on result of drift check, see {@link
   *     ConfigDrift.Result}
   * @param logResults if set to true, log config drift check results
   * @return a list containing topics whose config has drifted
   */
  List<ConfiguredTopic> topicsWithConfigDrift(
      Type type, Set<Result> safetyFilters, boolean logResults) {
    if (this.configured.isEmpty()) {
      return Collections.emptyList();
    }
    Map<String, ConfiguredTopic> existing = this.topicService.listExisting(true);
    return Collections.unmodifiableList(
        this.configured.stream()
            .filter(t -> existing.containsKey(t.getName()))
            .filter(
                t -> {
                  Result result = configDrift.check(t, existing.get(t.getName()), type);
                  if (logResults) {
                    String msgFmt = "Found {} for topic {}, in {} drift detection mode";
                    switch(result) {
                      case NO_DRIFT:
                        break;
                      case SAFE_DRIFT:
                        LOG.info(msgFmt, result, t.getName(), type);
                        break;
                      case UNSAFE_DRIFT:
                      case UNSUPPORTED_DRIFT:
                        LOG.warn(msgFmt, result, t.getName(), type);
                        break;
                    }
                  }
                  return safetyFilters.contains(result);
                })
            .collect(Collectors.toList()));
  }

  private List<ConfiguredTopic> topicsWithConfigDrift(Type type, Result safetyFilter) {
    return topicsWithConfigDrift(type, EnumSet.of(safetyFilter), false);
  }

  /**
   * Increase partitions for topics which have drifted from configuration.
   *
   * @return a list of topics for which partitions were increased
   */
  List<ConfiguredTopic> increasePartitions() {
    List<ConfiguredTopic> toIncrease =
        topicsWithConfigDrift(Type.PARTITION_COUNT, configDriftSafetyFilters, true);
    if (!toIncrease.isEmpty()) {
      LOG.info("Increasing partitions for {} topics: {}", toIncrease.size(), toIncrease);
      topicService.increasePartitions(toIncrease);
    } else {
      LOG.info("No partition count drift was detected.");
    }
    return toIncrease;
  }

  /**
   * Alter topic configuration for topics which have drifted from configuration.
   *
   * @return a list of topics which were altered
   */
   List<ConfiguredTopic> alterConfiguration() {
    List<ConfiguredTopic> toAlter =
        topicsWithConfigDrift(Type.TOPIC_CONFIG, configDriftSafetyFilters, true);
    if (!toAlter.isEmpty()) {
      LOG.info("Altering topic configuration for {} topics: {}", toAlter.size(), toAlter);
      topicService.alterConfiguration(toAlter);
    } else {
      LOG.info("No topic configuration drift was detected.");
    }
    return toAlter;
  }

  @Override
  protected void alterDrifted() {
    increasePartitions();
    alterConfiguration();
  }

  @Override
  protected void create(List<ConfiguredTopic> toCreate) {
    topicService.create(toCreate);
  }

  @Override
  protected void delete(List<ConfiguredTopic> toDelete) {
    topicService.delete(toDelete);
  }

  @Override
  public void stats() {
    super.stats();
    partitionCountDrift
        .labels("safe")
        .set(topicsWithConfigDrift(Type.PARTITION_COUNT, Result.SAFE_DRIFT).size());
    partitionCountDrift
        .labels("unsafe")
        .set(topicsWithConfigDrift(Type.PARTITION_COUNT, Result.UNSAFE_DRIFT).size());
    topicConfigDrift
        .labels("safe")
        .set(topicsWithConfigDrift(Type.TOPIC_CONFIG, Result.SAFE_DRIFT).size());
    topicConfigDrift
        .labels("unsafe")
        .set(topicsWithConfigDrift(Type.TOPIC_CONFIG, Result.UNSAFE_DRIFT).size());
    unsupportedDrift
        .labels("replication_drift")
        .set(topicsWithConfigDrift(Type.REPLICATION_FACTOR, Result.UNSUPPORTED_DRIFT).size());
  }

}
