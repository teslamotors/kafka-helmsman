/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static com.tesla.data.topic.enforcer.ConfigDrift.Result.NO_DRIFT;
import static com.tesla.data.topic.enforcer.ConfigDrift.Result.SAFETY_UNKNOWN_DRIFT;
import static com.tesla.data.topic.enforcer.ConfigDrift.Result.SAFE_DRIFT;
import static com.tesla.data.topic.enforcer.ConfigDrift.Result.UNSAFE_DRIFT;
import static com.tesla.data.topic.enforcer.ConfigDrift.Result.UNSUPPORTED_DRIFT;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class ConfigDriftTest {

  private final ConfigDrift drift = new ConfigDrift(10, 100, 0.25f, Collections.emptySet());
  private final ConfiguredTopic actual = new ConfiguredTopic("a", 10, (short) 1, Collections.emptyMap());

  @Test
  public void testNoDrift() {
    // No drift of any kind
    Assert.assertEquals(NO_DRIFT, drift.check(actual, actual, ConfigDrift.Type.ANY));

    // Drift in topic properties but not in partition count
    ConfiguredTopic desired = new ConfiguredTopic("a", 10, (short) 1, Collections.singletonMap("k", "v"));
    Assert.assertEquals(NO_DRIFT, drift.check(desired, actual, ConfigDrift.Type.PARTITION_COUNT));

    // Drift in partition count but not in properties
    desired = new ConfiguredTopic("a", 20, (short) 1, Collections.emptyMap());
    Assert.assertEquals(NO_DRIFT, drift.check(desired, actual, ConfigDrift.Type.TOPIC_CONFIG));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInValidCheck() {
    // Checking a different topic
    ConfiguredTopic desired = new ConfiguredTopic("b", 1, (short) 1, Collections.emptyMap());
    drift.check(desired, actual, ConfigDrift.Type.ANY);
  }

  @Test
  public void testUnknownSafetyDrift() {
    ConfiguredTopic desired = new ConfiguredTopic("a", 20, (short) 1, Collections.emptyMap());
    Assert.assertEquals(SAFETY_UNKNOWN_DRIFT, drift.check(desired, actual, ConfigDrift.Type.ANY));
  }

  @Test
  public void testSafePartitionIncrease() {
    // 20% increase in partition count
    ConfiguredTopic desired = new ConfiguredTopic("a", actual.getPartitions() + 2, (short) 1, Collections.emptyMap());
    Assert.assertEquals(SAFE_DRIFT, drift.check(desired, actual, ConfigDrift.Type.PARTITION_COUNT));
  }

  @Test
  public void testSafeTopicProperties() {
    ConfiguredTopic desired = new ConfiguredTopic("a", 10, (short) 1, Collections.singletonMap("k", "v"));
    Assert.assertEquals(SAFE_DRIFT, drift.check(desired, actual, ConfigDrift.Type.TOPIC_CONFIG));
  }

  @Test
  public void testUnSafePartitionIncrease() {
    ConfiguredTopic desired = new ConfiguredTopic("a", 20, (short) 1, Collections.emptyMap());
    Assert.assertEquals(UNSAFE_DRIFT, drift.check(desired, actual, ConfigDrift.Type.PARTITION_COUNT));
  }

  @Test
  public void testUnSafeTopicProperties() {
    ConfigDrift drift = new ConfigDrift(10, 20, 0.25f, Collections.singleton("k"));
    ConfiguredTopic desired = new ConfiguredTopic("a", 10, (short) 1, Collections.singletonMap("k", "v"));
    Assert.assertEquals(UNSAFE_DRIFT, drift.check(desired, actual, ConfigDrift.Type.TOPIC_CONFIG));
  }

  @Test
  public void testReplicatioFactorDrift() {
	  ConfiguredTopic desired = new ConfiguredTopic("a", 10, (short) 2, Collections.emptyMap());
	  Assert.assertEquals(UNSUPPORTED_DRIFT, drift.check(desired, actual, ConfigDrift.Type.REPLICATION_FACTOR));
  }

  @Test
  public void testReplicatioFactorNoDrift() {
	  ConfiguredTopic desired = new ConfiguredTopic("a", 10, (short) 1, Collections.emptyMap());
	  Assert.assertEquals(NO_DRIFT, drift.check(desired, actual, ConfigDrift.Type.REPLICATION_FACTOR));
  }

  @Test
  public void testLowWaterMark() {
    ConfigDrift drift = new ConfigDrift(100, 200, 0.25f, Collections.emptySet());
    // 5x-ing the partition count but lower than low water mark, so it should be safe
    ConfiguredTopic desired = new ConfiguredTopic("a", 50, (short) 1, Collections.emptyMap());
    Assert.assertEquals(SAFE_DRIFT, drift.check(desired, actual, ConfigDrift.Type.PARTITION_COUNT));
  }

  @Test
  public void testHighWaterMark() {
    ConfigDrift drift = new ConfigDrift(100, 200, 0.50f, Collections.emptySet());
    ConfiguredTopic actual = new ConfiguredTopic("a", 150, (short) 1, Collections.emptyMap());
    // increasing partition count by ~ 35%, allowed is 50% but crosses the high water mark
    ConfiguredTopic desired = new ConfiguredTopic("a", 201, (short) 1, Collections.emptyMap());
    Assert.assertEquals(UNSAFE_DRIFT, drift.check(desired, actual, ConfigDrift.Type.PARTITION_COUNT));
  }

}
