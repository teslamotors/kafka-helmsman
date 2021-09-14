/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static tesla.shade.com.google.common.base.Preconditions.checkArgument;
import static tesla.shade.com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Set;

/**
 * Checks config drift between desired and actual topic configuration.
 */
public class ConfigDrift {

  // permissible % increase threshold doesnt apply if the increased amount is below low water mark
  private final int partitionCountLowWaterMark;
  // an increase above the high water mark is considered un-safe even if its below the permissible % increase threshold
  private final int partitionCountHighWaterMark;
  // permissible % increase in partition count, value between (0, 1]
  private final float partitionIncreaseThreshold;
  // a set of properties which are considered unsafe
  private final Set<String> unsafeConfigProperties;

  public ConfigDrift(int partitionCountLowWaterMark, int partitionCountHighWaterMark,
                     float partitionIncreaseThreshold, Set<String> unsafeConfigProperties) {
    checkArgument(partitionIncreaseThreshold > 0 && partitionIncreaseThreshold <= 1);
    checkArgument(partitionCountLowWaterMark > 0 && partitionCountHighWaterMark > 0);
    this.partitionCountLowWaterMark = partitionCountLowWaterMark;
    this.partitionCountHighWaterMark = partitionCountHighWaterMark;
    this.partitionIncreaseThreshold = partitionIncreaseThreshold;
    this.unsafeConfigProperties = checkNotNull(unsafeConfigProperties);;
  }

  public ConfigDrift() {
    this(50, 1000 ,0.25f, Collections.emptySet());
  }

  enum Type {
    // Check drift of any kid
    ANY,
    // Check drift based on partition count alone
    PARTITION_COUNT,
    // Check drift based on topic properties alone
    TOPIC_CONFIG,
    // Check drift based on replication factor alone
    REPLICATION_FACTOR
  }

  enum Result {
    // No changes between desired and actual topic config
    NO_DRIFT,
    // Changes detected and were found to be safe
    SAFE_DRIFT,
    // Changes detected and were found to be un-safe
    UNSAFE_DRIFT,
    // Changes detected, but it wasn't clear if they are safe or not
    SAFETY_UNKNOWN_DRIFT,
    // Changes detected but enforcement for them is unsupported
    UNSUPPORTED_DRIFT
  }

  /**
   * Check if there is a config drift between two topic configuration states.
   *
   * @param desired the desired {@link ConfiguredTopic}
   * @param actual an actual {@link ConfiguredTopic}
   * @param type the checking mode, see {@link Type}
   * @return a result indicating drift along with a sense of safety
   */
  public Result check(ConfiguredTopic desired, ConfiguredTopic actual, Type type) {
    checkArgument(desired.getName().equals(actual.getName()),
        "Config drift check should only be applied to same topics!");
    switch (type) {
      case ANY:
        return desired.equals(actual) ? Result.NO_DRIFT : Result.SAFETY_UNKNOWN_DRIFT;

      case PARTITION_COUNT:
        if (desired.getPartitions() == actual.getPartitions()) {
          return Result.NO_DRIFT;
        } else {
          if (desired.getPartitions() <= partitionCountLowWaterMark) {
            return Result.SAFE_DRIFT;
          } else if (desired.getPartitions() > partitionCountHighWaterMark) {
            return Result.UNSAFE_DRIFT;
          }
          float increase = (desired.getPartitions() - actual.getPartitions()) * 1.0f / actual.getPartitions();
          return increase <= partitionIncreaseThreshold ? Result.SAFE_DRIFT :  Result.UNSAFE_DRIFT;
        }

      case TOPIC_CONFIG:
        if (desired.getConfig().equals(actual.getConfig())) {
          return Result.NO_DRIFT;
        } else {
          if (Collections.disjoint(desired.getConfig().keySet(), unsafeConfigProperties)) {
            return Result.SAFE_DRIFT;
          } else {
            return Result.UNSAFE_DRIFT;
          }
        }

      case REPLICATION_FACTOR:
        return desired.getReplicationFactor() == actual.getReplicationFactor() ? Result.NO_DRIFT
            : Result.UNSUPPORTED_DRIFT;

      default:
        throw new IllegalStateException("Unknown config drift!");
    }
  }

}
