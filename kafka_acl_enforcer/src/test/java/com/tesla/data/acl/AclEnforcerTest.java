/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import static java.util.Collections.emptyList;
import static org.apache.kafka.common.resource.PatternType.LITERAL;
import static org.apache.kafka.common.resource.PatternType.PREFIXED;
import static org.apache.kafka.common.resource.ResourceType.CLUSTER;
import static org.apache.kafka.common.resource.ResourceType.GROUP;
import static org.apache.kafka.common.resource.ResourceType.TOPIC;
import static org.apache.kafka.common.resource.ResourceType.TRANSACTIONAL_ID;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tesla.data.enforcer.UnmanagedPrefixes;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AclEnforcerTest {

  private static final UnmanagedPrefixes UNMANAGED = UnmanagedPrefixes.from(Map.of(
      "topicPrefixes", List.of("ext-"),
      "groupPrefixes", List.of("extgrp-")));

  private AclService aclService;
  private AclEnforcer enforcer;
  private List<AclBinding> acls;

  private static AclBinding binding(ResourceType type, String name, PatternType patternType) {
    return new AclBinding(
        new ResourcePattern(type, name, patternType),
        new AccessControlEntry("User:foo", "*", AclOperation.ALL, AclPermissionType.ALLOW));
  }

  @Before
  public void setup() throws IOException {
    aclService = mock(AclService.class);
    enforcer = new AclEnforcer(emptyList(), aclService, true, true);
    acls = Arrays.asList(AclConfig.bindingsForTest());
  }

  @Test
  public void testCreateDryRun() throws IOException {
    enforcer.create(acls);
    verify(aclService, times(0)).create(anyCollection());
  }

  @Test
  public void testDeleteDryRun() throws IOException {
    enforcer.delete(acls);
    verify(aclService, times(0)).delete(anyCollection());
  }

  @Test
  public void testUnexpectedSkipsUnmanagedResources() {
    AclBinding managedTopic = binding(TOPIC, "orders", LITERAL);
    AclBinding unmanagedTopic = binding(TOPIC, "ext-orders", LITERAL);
    AclBinding unmanagedTopicPrefixed = binding(TOPIC, "ext-team1-", PREFIXED);
    // grants more than the unmanaged namespace, hence still enforced
    AclBinding broaderPrefixed = binding(TOPIC, "ext", PREFIXED);
    AclBinding unmanagedGroup = binding(GROUP, "extgrp-reader", LITERAL);
    // topic prefixes do not apply to groups
    AclBinding groupWithTopicPrefix = binding(GROUP, "ext-reader", LITERAL);
    AclBinding cluster = binding(CLUSTER, "kafka-cluster", LITERAL);
    AclBinding transactionalId = binding(TRANSACTIONAL_ID, "ext-txn", LITERAL);
    when(aclService.listExisting()).thenReturn(List.of(managedTopic, unmanagedTopic, unmanagedTopicPrefixed,
        broaderPrefixed, unmanagedGroup, groupWithTopicPrefix, cluster, transactionalId));

    AclEnforcer enforcer = new AclEnforcer(emptyList(), aclService, UNMANAGED, true, true);

    Assert.assertEquals(
        Set.of(managedTopic, broaderPrefixed, groupWithTopicPrefix, cluster, transactionalId),
        new HashSet<>(enforcer.unexpected()));
  }

  @Test
  public void testNoUnmanagedPrefixesKeepsAllUnexpected() {
    AclBinding internalTopic = binding(TOPIC, "_internal", LITERAL);
    AclBinding prefixedTopic = binding(TOPIC, "ext-orders", LITERAL);
    when(aclService.listExisting()).thenReturn(List.of(internalTopic, prefixedTopic));

    AclEnforcer enforcer = new AclEnforcer(emptyList(), aclService, true, true);

    Assert.assertEquals(Set.of(internalTopic, prefixedTopic), new HashSet<>(enforcer.unexpected()));
  }

  @Test
  public void testWildcardBindingsAlwaysManaged() {
    UnmanagedPrefixes star = UnmanagedPrefixes.from(Map.of(
        "topicPrefixes", List.of("*"),
        "groupPrefixes", List.of("*")));
    AclBinding wildcardTopic = binding(TOPIC, ResourcePattern.WILDCARD_RESOURCE, LITERAL);
    AclBinding wildcardGroup = binding(GROUP, ResourcePattern.WILDCARD_RESOURCE, LITERAL);
    when(aclService.listExisting()).thenReturn(List.of(wildcardTopic, wildcardGroup));

    AclEnforcer enforcer = new AclEnforcer(emptyList(), aclService, star, true, true);

    Assert.assertEquals(Set.of(wildcardTopic, wildcardGroup), new HashSet<>(enforcer.unexpected()));
  }

  @Test
  public void testConfiguredUnmanagedBindingIsStillCreated() {
    AclBinding unmanagedTopic = binding(TOPIC, "ext-orders", LITERAL);
    when(aclService.listExisting()).thenReturn(emptyList());

    AclEnforcer enforcer = new AclEnforcer(List.of(unmanagedTopic), aclService, UNMANAGED, true, false);
    enforcer.createAbsent();

    verify(aclService).create(List.of(unmanagedTopic));
  }

  @Test
  public void testDeleteUnexpectedLeavesUnmanagedBindings() {
    AclBinding keepA = binding(TOPIC, "a", LITERAL);
    AclBinding keepB = binding(TOPIC, "b", LITERAL);
    AclBinding stale = binding(TOPIC, "stale", LITERAL);
    AclBinding external = binding(TOPIC, "ext-orders", LITERAL);
    when(aclService.listExisting()).thenReturn(List.of(keepA, keepB, stale, external));

    // without the unmanaged filter, 2 of 2 configured would be deleted and trip the 50% threshold
    AclEnforcer enforcer = new AclEnforcer(List.of(keepA, keepB), aclService, UNMANAGED, false, false);
    enforcer.deleteUnexpected();

    verify(aclService).delete(List.of(stale));
  }

  @Test
  public void testLogsEffectiveUnmanagedPrefixes() {
    // a misspelled 'unmanaged' section is silently ignored, the startup log is how an operator notices
    Logger logger = (Logger) LoggerFactory.getLogger(AclEnforcer.class);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    logger.addAppender(appender);
    try {
      new AclEnforcer(emptyList(), aclService, UNMANAGED, true, true);
    } finally {
      logger.detachAppender(appender);
    }
    Assert.assertTrue(appender.list.stream()
        .map(ILoggingEvent::getFormattedMessage)
        .anyMatch("Unmanaged topic prefixes: [ext-], group prefixes: [extgrp-]"::equals));
  }
}
