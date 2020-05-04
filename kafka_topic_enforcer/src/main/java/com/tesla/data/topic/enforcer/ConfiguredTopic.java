/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static tesla.shade.com.google.common.base.Preconditions.checkArgument;
import static tesla.shade.com.google.common.base.Preconditions.checkNotNull;

import tesla.shade.com.google.common.base.MoreObjects;
import tesla.shade.com.google.common.base.Objects;

import java.util.HashMap;
import java.util.Map;

/**
 * ConfiguredTopic represents a topic and its desired configuration.
 */
public class ConfiguredTopic {

  private final String name;
  private final int partitions;
  private final short replicationFactor;
  private final Map<String, String> config;

  // Only meant for cosmetic labeling of a topic in the enforcer config, its not passed to the
  // brokers
  private Map<String, Object> tags = new HashMap<>();

  public ConfiguredTopic(String name, int partitions,
                         short replicationFactor, Map<String, String> config) {
    this.name = checkNotNull(name, "topic name should be non null");
    checkArgument(partitions > 0, "%s is configured with invalid partitions: %s", name, partitions);
    checkArgument(replicationFactor > 0,
        "%s is configured with invalid replicationFactor: %s", name, replicationFactor);
    this.partitions = partitions;
    this.replicationFactor = replicationFactor;
    this.config = config == null ? new HashMap<>() : config;
  }

  public String getName() {
    return name;
  }

  public int getPartitions() {
    return partitions;
  }

  public short getReplicationFactor() {
    return replicationFactor;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public Map<String, Object> getTags() {
    return tags;
  }

  // Unlike the mandatory fields, optional fields needs a setter
  // see https://github.com/FasterXML/jackson-modules-java8/tree/master/parameter-names
  public void setTags(Map<String, Object> tags) {
    this.tags = tags;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("partitions", partitions)
        .add("replicationFactor", replicationFactor)
        .add("config", config)
        .add("tags", tags)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfiguredTopic that = (ConfiguredTopic) o;
    return partitions == that.partitions &&
        replicationFactor == that.replicationFactor &&
        Objects.equal(name, that.name) &&
        Objects.equal(config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, partitions, replicationFactor, config);
  }
}
