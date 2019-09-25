/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;


import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;


public class EnforcerTest {

  private final List<ConfiguredTopic> configured = Arrays.asList(
      new ConfiguredTopic("topic_a", 1, (short) 1, Collections.emptyMap()),
      new ConfiguredTopic("topic_b", 1, (short) 2, Collections.emptyMap()));
  private TopicService service;
  private Enforcer enforcer;

  @Before
  public void setup() {
    service = mock(TopicService.class);
    enforcer = new Enforcer(service, configured, true);
  }

  @Test
  public void testPassesSanityCheckGood() {
    Assert.assertTrue(Enforcer.passesSanityCheck(configured));
  }

  @Test
  public void testPassesSanityCheckBad() {
    Assert.assertFalse(Enforcer.passesSanityCheck(Collections.nCopies(2, configured.get(0))));
  }

  @Test
  public void testPassesSanityCheckBadWithInternalTopic() {
    List<ConfiguredTopic> configured = Arrays.asList(
        new ConfiguredTopic("_internal_topic", 1, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("topic_a", 1, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("topic_b", 1, (short) 2, Collections.emptyMap()));
    Assert.assertFalse(Enforcer.passesSanityCheck(configured));
  }

  @Test
  public void testAbsentTopics() {
    when(service.listExisting(true)).thenReturn(Collections.singletonMap("topic_a",
        new ConfiguredTopic("topic_a", 1, (short) 1, Collections.emptyMap())));
    Assert.assertEquals(Collections.singletonList(configured.get(1)), enforcer.absentTopics());
  }


  @Test
  public void testAbsentTopicsNoneCreated() {
    when(service.listExisting(true)).thenReturn(Collections.emptyMap());
    Assert.assertEquals(configured, enforcer.absentTopics());
  }

  @Test
  public void testAbsentTopicsNoneConfigured() {
    TopicService service = mock(TopicService.class);
    Enforcer enforcer = new Enforcer(service, Collections.emptyList(), true);
    Assert.assertTrue(enforcer.absentTopics().isEmpty());
    verifyZeroInteractions(service);
  }

  @Test
  public void testPartitionIncrease() {
    List<ConfiguredTopic> configured = Arrays.asList(
        new ConfiguredTopic("a", 1, (short) 3, Collections.emptyMap()),
        // topic 'b' wants to increase partition count to 3
        new ConfiguredTopic("b", 3, (short) 1, Collections.emptyMap()),
        new ConfiguredTopic("c", 1, (short) 1, Collections.emptyMap()));

    Map<String, ConfiguredTopic> existing = new HashMap<String, ConfiguredTopic>() {
      {
        // cluster topic has less replicas than configured but same partitions, we do not care!
        put("a", new ConfiguredTopic("a", 1, (short) 1, Collections.emptyMap()));
        // cluster topic has less partitions than configured, we care!
        put("b", new ConfiguredTopic("b", 1, (short) 1, Collections.emptyMap()));
        // cluster has an extra topic that is not configured to be present, we do not care!
        put("d", new ConfiguredTopic("b", 1, (short) 1, Collections.emptyMap()));
      }
    };

    when(service.listExisting(true)).thenReturn(existing);
    Enforcer enforcer = new Enforcer(service, configured, true);
    enforcer.increasePartitions();

    // topic 'b'
    List<ConfiguredTopic> expected = Collections.singletonList(configured.get(1));
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
        new ConfiguredTopic("c", 1, (short) 1, Collections.emptyMap()));

    Map<String, ConfiguredTopic> existing = new HashMap<String, ConfiguredTopic>() {
      {
        // cluster topic has as some config overrides
        put("a", new ConfiguredTopic("a", 1, (short) 1, Collections.singletonMap("k", "v")));
        // no change in topic config, partition count should be ignored
        put("b", new ConfiguredTopic("b", 3, (short) 1, Collections.emptyMap()));
        // topic 'c' is no present in the cluster
      }
    };

    when(service.listExisting(true)).thenReturn(existing);
    Enforcer enforcer = new Enforcer(service, configured, true);
    enforcer.alterConfiguration();

    // topic 'a'
    List<ConfiguredTopic> expected = Collections.singletonList(configured.get(0));
    verify(service).listExisting(true);
    verify(service).alterConfiguration(expected);
    verifyNoMoreInteractions(service);
  }

  @Test
  public void testNoUnexpectedTopics() {
    Map<String, ConfiguredTopic> existing =
        configured.stream().collect(Collectors.toMap(ConfiguredTopic::getName, Function.identity()));
    when(service.listExisting(true)).thenReturn(existing);
    Assert.assertTrue(enforcer.unexpectedTopics().isEmpty());
  }

  @Test
  public void testUnexpectedTopics() {
    Map<String, ConfiguredTopic> existing = Collections.singletonMap("x",
        new ConfiguredTopic("x", 1, (short) 1, Collections.emptyMap()));
    when(service.listExisting(true)).thenReturn(existing);
    Assert.assertEquals(1, enforcer.unexpectedTopics().size());
    Assert.assertEquals(existing.get("x"), enforcer.unexpectedTopics().get(0));
  }

  @Test(expected = IllegalStateException.class)
  public void testSafeMode() {
    enforcer = new Enforcer(service, configured, true);
    enforcer.deleteUnexpectedTopics();
  }

  @Test
  public void testSafeModeEnforceAll() {
    Map<String, ConfiguredTopic> existing = Collections.singletonMap("x",
        new ConfiguredTopic("x", 1, (short) 1, Collections.emptyMap()));
    when(service.listExisting(true)).thenReturn(existing);

    // safemode on
    new Enforcer(service, configured, true).enforceAll();
    verify(service, times(0)).delete(anyList());

    // safemode off
    new Enforcer(service, configured, false).enforceAll();
    verify(service, times(1)).delete(Collections.singletonList(existing.get("x")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyConfigSafeModeOff() {
    enforcer = new Enforcer(service, Collections.emptyList(), false);
  }

  @Test(expected = IllegalStateException.class)
  public void testTooManyTopicsBeingDeleted() {
    enforcer = new Enforcer(service, configured, false);
    Map<String, ConfiguredTopic> existing = new HashMap<String, ConfiguredTopic>() {
      {
        put("x", new ConfiguredTopic("x", 1, (short) 1, Collections.emptyMap()));
        put("y", new ConfiguredTopic("y", 1, (short) 1, Collections.emptyMap()));
      }
    };
    when(service.listExisting(true)).thenReturn(existing);

    // should not be allowed as we are deleting lot of topics!
    enforcer.deleteUnexpectedTopics();
  }

}