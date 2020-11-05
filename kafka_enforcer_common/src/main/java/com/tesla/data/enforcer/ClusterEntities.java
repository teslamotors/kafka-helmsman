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
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
   * @param allEntities     a list containing all consolidated (all clusters) configurations
   * @param cluster         the name of the cluster to filter Entities on
   * @param clusterDefaults default configuration values for the entity in the cluster
   * @return a list of entity configurations for the given cluster
   * @throws IllegalArgumentException if invalid config structure is passed
   */
  public static List<Map<String, Object>> forCluster(
      List<Map<String, Object>> allEntities, String cluster, Optional<Map<String, Object>> clusterDefaults) {
    Map<String, Object> defaults = clusterDefaults(clusterDefaults, cluster);
    return allEntities.stream()
        .peek(tc -> checkArgument(tc.containsKey(CLUSTERS), "%s does not contain cluster overrides", tc))
        // only include entities that have this cluster key
        .filter(tc ->{
          Map<String, Map<String, Object>> clusters =
              (Map<String, Map<String, Object>>) tc.get(CLUSTERS);
          return clusters.containsKey(cluster);
        }).map( tc ->{
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
          // base configuration is defaults -> cluster defaults -> topic base -> cluster topic configs
          Map<String, Object> baseConfig = applyOverrides(new HashMap<>(defaults), tc);
          baseConfig.remove(CLUSTERS);

          Map<String, Map<String, Object>> clusters =
              (Map<String, Map<String, Object>>) tc.get(CLUSTERS);
          // add the config for the given cluster
          return applyOverrides(baseConfig, clusters.get(cluster));
        }).collect(Collectors.toList());
  }

  private static Map<String, Object> clusterDefaults(Optional<Map<String, Object>> maybeDefaults, String cluster){
    if(!maybeDefaults.isPresent()){
      return Collections.emptyMap();
    }
    Map<String, Object> defaults = new HashMap<>(maybeDefaults.get());
    Map<String, Object> clusterDefaults = Collections.emptyMap();
    if(defaults.containsKey(CLUSTERS)){
      clusterDefaults = (Map<String, Object>) defaults.get(CLUSTERS);
      defaults.remove(CLUSTERS);
    }

    return applyOverrides(defaults, (Map<String, Object>) clusterDefaults.getOrDefault(cluster, Collections.emptyMap()));
  }


  // override all properties
  static Map<String, Object> applyOverrides(
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
      Map ret = new HashMap<>((Map) originalValue);
      ret.putAll((Map) overrideValue);
      return ret;
    }
    return overrideValue;
  }
}
