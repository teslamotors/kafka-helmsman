/*
 * Copyright (c) 2026. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UnmanagedPrefixesTest {

  private static final UnmanagedPrefixes PREFIXES = UnmanagedPrefixes.from(Map.of(
      "topicPrefixes", List.of("ext-", "vendor-"),
      "groupPrefixes", List.of("extgrp-")));

  @Test
  public void testAbsentSectionIsNone() {
    UnmanagedPrefixes prefixes = UnmanagedPrefixes.from(null);
    Assert.assertSame(UnmanagedPrefixes.NONE, prefixes);
    Assert.assertTrue(prefixes.topicPrefixes().isEmpty());
    Assert.assertTrue(prefixes.groupPrefixes().isEmpty());
  }

  @Test
  public void testParsesPrefixes() {
    Assert.assertEquals(Set.of("ext-", "vendor-"), PREFIXES.topicPrefixes());
    Assert.assertEquals(Set.of("extgrp-"), PREFIXES.groupPrefixes());
  }

  @Test
  public void testMissingKeyIsEmpty() {
    UnmanagedPrefixes prefixes = UnmanagedPrefixes.from(Map.of("topicPrefixes", List.of("ext-")));
    Assert.assertEquals(Set.of("ext-"), prefixes.topicPrefixes());
    Assert.assertTrue(prefixes.groupPrefixes().isEmpty());
  }

  @Test
  public void testNullListIsEmpty() {
    Map<String, Object> section = new HashMap<>();
    section.put("topicPrefixes", null);
    Assert.assertTrue(UnmanagedPrefixes.from(section).topicPrefixes().isEmpty());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsNonMapSection() {
    UnmanagedPrefixes.from(List.of("ext-"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsUnknownKey() {
    // a typo must not silently disable protection of external resources
    UnmanagedPrefixes.from(Map.of("topicprefixes", List.of("ext-")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsNonListValue() {
    UnmanagedPrefixes.from(Map.of("topicPrefixes", "ext-"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsNonStringPrefix() {
    UnmanagedPrefixes.from(Map.of("topicPrefixes", List.of(123)));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsEmptyPrefix() {
    // an empty prefix would match every resource
    UnmanagedPrefixes.from(Map.of("topicPrefixes", List.of("")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsBlankPrefix() {
    UnmanagedPrefixes.from(Map.of("groupPrefixes", List.of("   ")));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRejectsSurroundingWhitespace() {
    // a padded prefix would silently never match a real resource name
    UnmanagedPrefixes.from(Map.of("topicPrefixes", List.of("ext- ")));
  }

  @Test
  public void testMatching() {
    Assert.assertTrue(PREFIXES.isUnmanagedTopic("ext-orders"));
    Assert.assertTrue(PREFIXES.isUnmanagedTopic("vendor-orders"));
    Assert.assertTrue(PREFIXES.isUnmanagedTopic("ext-"));
    Assert.assertFalse(PREFIXES.isUnmanagedTopic("ext"));
    Assert.assertFalse(PREFIXES.isUnmanagedTopic("EXT-orders"));
    Assert.assertFalse(PREFIXES.isUnmanagedTopic("orders"));
    Assert.assertTrue(PREFIXES.isUnmanagedGroup("extgrp-reader"));
    // topic prefixes do not apply to groups and vice versa
    Assert.assertFalse(PREFIXES.isUnmanagedGroup("ext-reader"));
    Assert.assertFalse(PREFIXES.isUnmanagedTopic("extgrp-orders"));
  }

  @Test
  public void testNoneMatchesNothing() {
    Assert.assertFalse(UnmanagedPrefixes.NONE.isUnmanagedTopic("_foo"));
    Assert.assertFalse(UnmanagedPrefixes.NONE.isUnmanagedGroup("foo"));
  }

  @Test
  public void testWithTopicPrefixReturnsNewInstance() {
    UnmanagedPrefixes extended = PREFIXES.withTopicPrefix("_");
    Assert.assertTrue(extended.isUnmanagedTopic("_internal"));
    Assert.assertTrue(extended.isUnmanagedTopic("ext-orders"));
    Assert.assertEquals(PREFIXES.groupPrefixes(), extended.groupPrefixes());
    Assert.assertFalse(PREFIXES.isUnmanagedTopic("_internal"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWithTopicPrefixRejectsEmpty() {
    UnmanagedPrefixes.NONE.withTopicPrefix("");
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testPrefixesAreUnmodifiable() {
    PREFIXES.topicPrefixes().add("x-");
  }
}
