/*
 * Copyright (c) 2026. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import static tesla.shade.com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Name prefixes of resources owned by an external system. Enforcers never delete or alter existing
 * resources whose names start with one of these prefixes.
 */
public final class UnmanagedPrefixes {

  public static final String CONFIG_KEY = "unmanaged";
  static final String TOPIC_PREFIXES_KEY = "topicPrefixes";
  static final String GROUP_PREFIXES_KEY = "groupPrefixes";
  private static final Set<String> SUPPORTED_KEYS = Set.of(TOPIC_PREFIXES_KEY, GROUP_PREFIXES_KEY);

  public static final UnmanagedPrefixes NONE =
      new UnmanagedPrefixes(Collections.emptySet(), Collections.emptySet());

  private final Set<String> topicPrefixes;
  private final Set<String> groupPrefixes;

  private UnmanagedPrefixes(Set<String> topicPrefixes, Set<String> groupPrefixes) {
    this.topicPrefixes = Collections.unmodifiableSet(new LinkedHashSet<>(topicPrefixes));
    this.groupPrefixes = Collections.unmodifiableSet(new LinkedHashSet<>(groupPrefixes));
  }

  /**
   * Parse the 'unmanaged' config section.
   *
   * @param section raw config value, null if the section is absent
   * @return parsed prefixes, {@link #NONE} if the section is absent
   * @throws IllegalArgumentException if the section is malformed
   */
  public static UnmanagedPrefixes from(Object section) {
    if (section == null) {
      return NONE;
    }
    checkArgument(section instanceof Map, "'%s' must be a map, got: %s", CONFIG_KEY, section);
    Map<?, ?> map = (Map<?, ?>) section;
    // a typo in a key must fail loudly, otherwise external resources silently lose protection
    Set<?> unknown = map.keySet().stream()
        .filter(k -> !(k instanceof String && SUPPORTED_KEYS.contains(k)))
        .collect(Collectors.toSet());
    checkArgument(unknown.isEmpty(), "Unknown keys under '%s': %s, supported keys are %s",
        CONFIG_KEY, unknown, SUPPORTED_KEYS);
    return new UnmanagedPrefixes(prefixes(map, TOPIC_PREFIXES_KEY), prefixes(map, GROUP_PREFIXES_KEY));
  }

  private static Set<String> prefixes(Map<?, ?> section, String key) {
    Object value = section.get(key);
    if (value == null) {
      return Collections.emptySet();
    }
    checkArgument(value instanceof List, "'%s.%s' must be a list of prefixes, got: %s", CONFIG_KEY, key, value);
    Set<String> prefixes = new LinkedHashSet<>();
    for (Object prefix : (List<?>) value) {
      checkArgument(prefix instanceof String, "'%s.%s' must contain only strings, got: %s", CONFIG_KEY, key, prefix);
      prefixes.add(validPrefix((String) prefix));
    }
    return prefixes;
  }

  private static String validPrefix(String prefix) {
    // an empty prefix matches everything, a padded one never matches a real resource name
    checkArgument(!prefix.isEmpty() && prefix.equals(prefix.strip()),
        "Unmanaged prefix must be non-empty and have no surrounding whitespace, got: '%s'", prefix);
    return prefix;
  }

  public Set<String> topicPrefixes() {
    return topicPrefixes;
  }

  public Set<String> groupPrefixes() {
    return groupPrefixes;
  }

  public boolean isUnmanagedTopic(String name) {
    return matches(topicPrefixes, name);
  }

  public boolean isUnmanagedGroup(String name) {
    return matches(groupPrefixes, name);
  }

  /**
   * Get a copy of these prefixes with an additional topic prefix.
   *
   * @param prefix a topic prefix
   * @return a new instance, this instance is left unchanged
   */
  public UnmanagedPrefixes withTopicPrefix(String prefix) {
    Set<String> topics = new LinkedHashSet<>(topicPrefixes);
    topics.add(validPrefix(prefix));
    return new UnmanagedPrefixes(topics, groupPrefixes);
  }

  private static boolean matches(Set<String> prefixes, String name) {
    return prefixes.stream().anyMatch(name::startsWith);
  }

  @Override
  public String toString() {
    return "UnmanagedPrefixes{topicPrefixes=" + topicPrefixes + ", groupPrefixes=" + groupPrefixes + "}";
  }
}
