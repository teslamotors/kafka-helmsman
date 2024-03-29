/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import tesla.shade.com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TopicEnforcerTest {

  private final List<ConfiguredTopic> configured = Arrays.asList(
      new ConfiguredTopic("topic_a", 1, (short) 1, Collections.emptyMap()),
      new ConfiguredTopic("topic_b", 1, (short) 2, Collections.emptyMap()));
  private TopicService service;
  private TopicEnforcer enforcer;

  @Before
  public void setup() {
    service = mock(TopicService.class);
    enforcer = new TopicEnforcer(service, configured, true);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPassesSanityCheckBadWithInternalTopic() {
    List<ConfiguredTopic> configured = Arrays.asList(
        new ConfiguredTopic("_internal_topic", 1, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("topic_a", 1, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("topic_b", 1, (short) 2, Collections.emptyMap()));
    enforcer = new TopicEnforcer(service, configured, true);
  }

  @Test
  public void testPartitionIncrease() {
    List<ConfiguredTopic> configured = Arrays.asList(
        new ConfiguredTopic("a", 1, (short) 3, Collections.emptyMap()),
        // topic 'b' wants to increase partition count to 3
        new ConfiguredTopic("b", 3, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("c", 3, (short) 3, Collections.emptyMap()));

    Map<String, ConfiguredTopic> existing = ImmutableMap.of(
        // cluster topic has less replicas than configured but same partitions, we do not care!
        "a", new ConfiguredTopic("a", 1, (short) 1, Collections.emptyMap()),
        // cluster topic has less partitions than configured, we care!
        "b", new ConfiguredTopic("b", 1, (short) 1, Collections.emptyMap()),
        // cluster topic has less partitions than configured and less replicas, we care (about the partitions)!
        "c", new ConfiguredTopic("c", 1, (short) 1, Collections.emptyMap()),
        // cluster has an extra topic that is not configured to be present, we do not care!
        "d", new ConfiguredTopic("b", 1, (short) 1, Collections.emptyMap())
    );

    when(service.listExisting(true)).thenReturn(existing);
    TopicEnforcer enforcer = new TopicEnforcer(service, configured, true);
    enforcer.increasePartitions();

    // topic 'b' and 'c' should be subject to config alteration
    List<ConfiguredTopic> expected = Arrays.asList(configured.get(1), configured.get(2));
    verify(service).listExisting(true);
    verify(service).increasePartitions(expected);
    verifyNoMoreInteractions(service);
  }

  @Test
  public void testAlterConfiguration() {
    List<ConfiguredTopic> configured = Arrays.asList(
        // topic 'a' wants to remove all config overrides
        new ConfiguredTopic("a", 1, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("b", 1, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("c", 1, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("d", 1, (short) 3, Collections.emptyMap()));

    Map<String, ConfiguredTopic> existing = ImmutableMap.of(
        // cluster topic has as some config overrides
        "a", new ConfiguredTopic("a", 1, (short) 1, Collections.singletonMap("k", "v")),
        // no change in topic config, partition count should be ignored
        "b", new ConfiguredTopic("b", 3, (short) 1, Collections.emptyMap()),
        // topic 'c' is no present in the cluster
        // cluster topic has some config overrides and less replicas, we care (about the config overrides)
        "d", new ConfiguredTopic("d", 1, (short) 1, Collections.singletonMap("k", "v"))
    );

    when(service.listExisting(true)).thenReturn(existing);
    TopicEnforcer enforcer = new TopicEnforcer(service, configured, true);
    enforcer.alterConfiguration();

    // topic 'a' and 'd' should be subject to config alteration
    List<ConfiguredTopic> expected = Arrays.asList(configured.get(0), configured.get(3));
    verify(service).listExisting(true);
    verify(service).alterConfiguration(expected);
    verifyNoMoreInteractions(service);
  }

  @Test
  public void testPartitionIncreaseUnSafeMode() {
    List<ConfiguredTopic> configured = Collections.singletonList(
        new ConfiguredTopic("a", 1000, (short) 3, Collections.emptyMap()));
    Map<String, ConfiguredTopic> existing = Collections.singletonMap("a",
        new ConfiguredTopic("a", 100, (short) 3, Collections.emptyMap()));
    when(service.listExisting(true)).thenReturn(existing);
    enforcer = new TopicEnforcer(service, configured, false);
    Assert.assertEquals("aggressive partition count increase must be allowed in unsafe mode", configured,
        enforcer.increasePartitions());
  }

  @Test
  public void testConfigUpdateUnSafeMode() {
    Map<String, String> risky = Collections.singletonMap("high_risk", "true");
    List<ConfiguredTopic> configured = Collections.singletonList(
        new ConfiguredTopic("a", 10, (short) 3, risky));
    Map<String, ConfiguredTopic> existing = Collections.singletonMap("a",
        new ConfiguredTopic("a", 10, (short) 3, Collections.emptyMap()));
    when(service.listExisting(true)).thenReturn(existing);

    // attempt a risk config change, it should go through
    enforcer = new TopicEnforcer(service, configured,
        new ConfigDrift(10, 100, 0.25f, risky.keySet()), false);
    Assert.assertEquals("risky config must be allowed in unsafe mode", configured, enforcer.alterConfiguration());
  }

}
