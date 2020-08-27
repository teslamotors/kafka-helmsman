/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import static tesla.shade.com.google.common.base.Preconditions.checkArgument;

import tesla.shade.com.google.common.collect.Sets;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A utility class to parse consolidated (multi-cluster) configurations. The structure of a consolidated config is,
 *
 * <pre>{@code
 * ---
 * clusters:
 *   foo:
 *       bootstrap.servers: 'foo_1:9092,foo_2:9092,foo_3:9092'
 *   bar:
 *       bootstrap.servers: 'bar_1:9092,bar_2:9092,bar_3:9092'
 * topics:
 *   - name: topic_A
 *     partitions: 30
 *     replicationFactor: 3
 *     clusters:
 *       foo: {}
 *       bar:
 *         replicationFactor: 2
 *   - name: topic_B
 *     partitions: 10
 *     replicationFactor: 3
 *     clusters:
 *       foo: {}
 *
 * ---
 *
 * }</pre>
 *
 * Rules for filtering Entities & applying cluster overrides to the base config are,
 *
 * <ul>
 * <li>An entry is applicable to a cluster if that cluster is present under entry's 'clusters'
 * attribute. If no changes to base config are desired, specify empty dict '{}'
 * <li>All properties can be overridden, except name
 * <li>Map (dictionary) properties are merged in way that preserves base settings
 * </ul>
 */
public class ClusterEntities {

  public static final String CLUSTERS = "clusters";

  /**
   * Get Entities configurations for a given cluster.
   *
   * @param allEntities a list containing all consolidated (all clusters) configurations
   * @param cluster     the name of the cluster to filter Entities on
   * @return a list of entity configurations for the given cluster
   * @throws IllegalArgumentException if invalid config structure is passed
   */
  public static List<Map<String, Object>> forCluster(
      List<Map<String, Object>> allEntities, String cluster) {
    Map<String, List<Map<String, Object>>> map = new HashMap<>();
    allEntities.forEach(
        tc -> {
          /*
          expected structure for an entry 'tc' is:
          {
            name: some_topic
            partitions: 10
            replicationFactor: 3
            clusters:
              foo: {}
              bar:
                replicationFactor: 2
          }
          */
          checkArgument(tc.containsKey(CLUSTERS), "%s does not contain cluster overrides", tc);
          Map<String, Map<String, Object>> clusters =
              (Map<String, Map<String, Object>>) tc.get(CLUSTERS);

          // base configuration is everything except the overrides under 'clusters' key
          Map<String, Object> baseConfig = new HashMap<>(tc);
          baseConfig.remove(CLUSTERS);

          clusters.forEach(
              (c, o) ->
                  map.computeIfAbsent(c, k -> new LinkedList<>())
                      .add(applyOverrides(baseConfig, o)));
        });

    return map.getOrDefault(cluster, Collections.emptyList());
  }

  // override all properties
  private static Map<String, Object> applyOverrides(
      Map<String, Object> base, Map<String, Object> overrides) {
    Map<String, Object> result = new HashMap<>(base);
    Set<String> properties = Sets.union(base.keySet(), overrides.keySet());
    properties.forEach(
        p -> {
          Object newValue = overrides.containsKey(p) ? overrides.get(p) : base.get(p);
          result.merge(p, newValue, ClusterEntities::remap);
        });
    return result;
  }

  // override a single property
  private static Object remap(Object originalValue, Object overrideValue) {
    if (originalValue instanceof Map) {
      checkArgument(overrideValue instanceof Map);
      ((Map) originalValue).putAll((Map) overrideValue);
      return originalValue;
    }
    return overrideValue;
  }
}
