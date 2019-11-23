/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static tesla.shade.com.google.common.base.Preconditions.checkArgument;
import static tesla.shade.com.google.common.base.Preconditions.checkState;

import com.tesla.data.topic.enforcer.ConfigDrift.Result;
import com.tesla.data.topic.enforcer.ConfigDrift.Type;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Enforce configured topics state on a kafka cluster.
 */
public class Enforcer {

  private final TopicService topicService;
  private final List<ConfiguredTopic> configuredTopics;
  private final ConfigDrift configDrift;
  private final Set<Result> configDriftSafetyFilters;

  // if safemode is set to true, operations such as delete are not performed
  private final boolean safemode;
  private static final Logger LOG = LoggerFactory.getLogger(Enforcer.class);
  // prometheus metrics should be static, see https://git.io/fj17x
  private static final EnforcerMetrics metrics = new EnforcerMetrics();
  // we do not allow more than this % of topics to be deleted in a single run
  private static final float PERMISSIBLE_DELETION_THRESHOLD = 0.50f;

  public Enforcer(TopicService topicService, List<ConfiguredTopic> configuredTopics,
                  ConfigDrift configDrift, boolean safemode) {
    if (!passesSanityCheck(configuredTopics)) {
      throw new IllegalArgumentException("Invalid configuration");
    }
    checkArgument(safemode || configuredTopics.size() != 0,
        "Configured topics should not be empty if safemode if off");
    this.topicService = topicService;
    this.configuredTopics = configuredTopics;
    this.configDrift = configDrift;
    this.safemode = safemode;
    this.configDriftSafetyFilters = safemode ? EnumSet.of(Result.SAFE_DRIFT) :
        EnumSet.of(Result.SAFE_DRIFT, Result.UNSAFE_DRIFT);
  }

  public Enforcer(TopicService topicService, List<ConfiguredTopic> configuredTopics, boolean safemode) {
    this(topicService, configuredTopics, new ConfigDrift(), safemode);
  }

  /**
   * Run sanity check on topic configuration. Check fails if
   * - duplicates are found
   * - TBD
   *
   * @param configuredTopics a list of configured topics
   * @return true if topic configuration passes all the checks
   */
  static boolean passesSanityCheck(List<ConfiguredTopic> configuredTopics) {
    boolean result = true;
    long distinct = configuredTopics.stream().distinct().count();
    if (configuredTopics.size() != distinct) {
      LOG.error("Found duplicate topic names in config");
      result = false;
    }
    if (configuredTopics.stream().anyMatch(TopicService.INTERNAL_TOPIC)) {
      LOG.error("Internal topics found in config");
      result = false;
    }
    return result;
  }

  /**
   * Find topics that are absent from the cluster.
   *
   * @return a list containing topics present in the config but absent in the cluster
   */
  public List<ConfiguredTopic> absentTopics() {
    if (this.configuredTopics.isEmpty()) {
      return Collections.emptyList();
    }
    Map<String, ConfiguredTopic> existing = this.topicService.listExisting(true);
    return Collections.unmodifiableList(
        this.configuredTopics
        .stream()
        .filter(t -> !existing.containsKey(t.getName()))
        .collect(Collectors.toList())
    );
  }

  /**
   * Create topics that are absent from the cluster.
   *
   * @return a list of newly created topics
   */
  public List<ConfiguredTopic> createAbsentTopics() {
    List<ConfiguredTopic> toCreate = absentTopics();
    if (!toCreate.isEmpty()) {
      LOG.info("Creating {} new topics: {}", toCreate.size(), toCreate);
      topicService.create(toCreate);
    } else {
      LOG.info("No new topic was discovered.");
    }
    return toCreate;
  }

  /**
   * Find topics whose actual configuration has drifted from expected configuration.
   * The returned list will have topic represented in the 'configured' aka 'desired' form.
   *
   * @param type the mode in which to perform config drift check, see {@link ConfigDrift.Type}
   * @param safetyFilters a set of filters to apply on result of drift check, see {@link ConfigDrift.Result}
   * @param logResults if set to true, log config drift check results
   * @return a list containing topics whose config has drifted
   */
  public List<ConfiguredTopic> topicsWithConfigDrift(Type type, Set<Result> safetyFilters, boolean logResults) {
    if (this.configuredTopics.isEmpty()) {
      return Collections.emptyList();
    }
    Map<String, ConfiguredTopic> existing = this.topicService.listExisting(true);
    return Collections.unmodifiableList(
        this.configuredTopics
            .stream()
            .filter(t -> existing.containsKey(t.getName()))
            .filter(t -> {
              Result result = configDrift.check(t, existing.get(t.getName()), type);
              if (logResults && !Result.NO_DRIFT.equals(result)) {
                LOG.info("Found {} for topic {}, in {} drift detection mode", result, t.getName(), type);
              }
              return safetyFilters.contains(result);
            })
            .collect(Collectors.toList())
    );
  }

  private List<ConfiguredTopic> topicsWithConfigDrift(Type type, Result safetyFilter) {
    return topicsWithConfigDrift(type, EnumSet.of(safetyFilter), false);
  }

  /**
   * Increase partitions for topics which have drifted from configuration.
   *
   * @return a list of topics for which partitions were increased
   */
  public List<ConfiguredTopic> increasePartitions() {
    List<ConfiguredTopic> toIncrease = topicsWithConfigDrift(Type.PARTITION_COUNT, configDriftSafetyFilters, true);
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
  public List<ConfiguredTopic> alterConfiguration() {
    List<ConfiguredTopic> toAlter = topicsWithConfigDrift(Type.TOPIC_CONFIG, configDriftSafetyFilters, true);
    if (!toAlter.isEmpty()) {
      LOG.info("Altering topic configuration for {} topics: {}", toAlter.size(), toAlter);
      topicService.alterConfiguration(toAlter);
    } else {
      LOG.info("No topic configuration drift was detected.");
    }
    return toAlter;
  }

  /**
   * Find topics that exist in the cluster but are absent from configuration.
   *
   * @return a list of {@link ConfiguredTopic}
   */
  public List<ConfiguredTopic> unexpectedTopics() {
    Map<String, ConfiguredTopic> existing = this.topicService.listExisting(true);
    Map<String, ConfiguredTopic> configured = this.configuredTopics
        .stream()
        .collect(Collectors.toMap(ConfiguredTopic::getName, Function.identity()));
    return Collections.unmodifiableList(
        existing.values()
            .stream()
            .filter(t -> !configured.containsKey(t.getName()))
            .collect(Collectors.toList()));
  }

  /**
   * Delete unexpected topics from the cluster. This operation is only supported when safemode
   * is turned off.
   *
   * @return a list of deleted topics
   */
  public List<ConfiguredTopic> deleteUnexpectedTopics() {
    checkState(!safemode, "Topic deletion is not allowed in safe mode");
    checkState(configuredTopics.size() > 0, "Configured topics can not be empty");
    List<ConfiguredTopic> toDelete = unexpectedTopics();
    float destruction = toDelete.size() * 1.0f / configuredTopics.size();
    checkState( destruction <= PERMISSIBLE_DELETION_THRESHOLD,
        "Too many [%s%] topics being deleted in one run", destruction * 100);
    if (!toDelete.isEmpty()) {
      LOG.info("Deleting {} redundant topics: {}", toDelete.size(), toDelete);
      topicService.delete(toDelete);
    } else {
      LOG.info("No un-expected topic was found.");
    }
    return toDelete;
  }


  public void enforceAll() {
    LOG.info("\n--Enforcement run started--");
    createAbsentTopics();
    increasePartitions();
    alterConfiguration();
    if (!safemode) {
      deleteUnexpectedTopics();
    }
    LOG.info("\n--Enforcement run ended--");
  }

  public EnforcerMetrics stats() {
    //TODO: In all of these calls we list all the topics from the cluster multiple times -- adding caching later.
    metrics.absentTopics.set(absentTopics().size());
    metrics.unexpectedTopics.set(unexpectedTopics().size());
    metrics.configuredTopics.set(configuredTopics.size());
    metrics.partitionCountDrift
        .labels("safe")
        .set(topicsWithConfigDrift(Type.PARTITION_COUNT, Result.SAFE_DRIFT).size());
    metrics.partitionCountDrift
        .labels("unsafe")
        .set(topicsWithConfigDrift(Type.PARTITION_COUNT, Result.UNSAFE_DRIFT).size());
    metrics.topicConfigDrift
        .labels("safe")
        .set(topicsWithConfigDrift(Type.TOPIC_CONFIG, Result.SAFE_DRIFT).size());
    metrics.topicConfigDrift
        .labels("unsafe")
        .set(topicsWithConfigDrift(Type.TOPIC_CONFIG, Result.UNSAFE_DRIFT).size());
    return metrics;
  }
}
