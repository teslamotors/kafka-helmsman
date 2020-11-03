/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Assert;
import org.junit.Test;
import tesla.shade.com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ClusterEntitiesTest {

  private static ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
  private static TypeReference<List<Map<String, Object>>> type =
      new TypeReference<List<Map<String, Object>>>() {
      };

  private String conf =
      String.join(
          "\n",
          new String[]{
              "---",
              "- name: topic_a",
              "  partitions: 1",
              "  replicationFactor: 1",
              "  clusters:",
              "      cluster_one: {}",
              "      cluster_two: ",
              "        replicationFactor: 2",
              "- name: topic_b",
              "  partitions: 1",
              "  replicationFactor: 1",
              "  clusters:",
              "      cluster_three: ",
              "        config: ",
              "          k1: v1",
              "      cluster_two: {}",
          });

  private String clusterOneFinal =
      String.join(
          "\n",
          new String[]{
              "---",
              "- name: topic_a",
              "  partitions: 1",
              "  replicationFactor: 1"
          });

  private String clusterTwoFinal =
      String.join(
          "\n",
          new String[]{
              "---",
              "- name: topic_a",
              "  partitions: 1",
              "  replicationFactor: 2",
              "- name: topic_b",
              "  partitions: 1",
              "  replicationFactor: 1",
          });

  private String clusterThreeFinal =
      String.join(
          "\n",
          new String[]{
              "---",
              "- name: topic_b",
              "  partitions: 1",
              "  replicationFactor: 1",
              "  config: ",
              "    k1: v1"
          });

  static List<Map<String, Object>> topics(String conf, String cluster) throws IOException {
    return topics(conf, cluster, null);
  }

  static List<Map<String, Object>> topics(String conf, String cluster, Map<String, Object> defaults) throws IOException {
    return ClusterEntities.forCluster(mapper.readValue(conf, type), cluster, Optional.ofNullable(defaults));
  }

  @Test
  public void testTopicsForCluster() throws IOException {
    Assert.assertEquals(mapper.readValue(clusterOneFinal, type), topics(conf, "cluster_one"));
    Assert.assertEquals(Collections.emptyList(), topics(conf, "cluster_four"));
    Assert.assertEquals(1, topics(conf, "cluster_three").size());
    Assert.assertEquals(2, topics(conf, "cluster_two").size());
  }

  @Test
  public void testNonMapPropertiesAreOverridden() throws IOException {
    Assert.assertEquals(mapper.readValue(clusterTwoFinal, type), topics(conf, "cluster_two"));
  }

  @Test
  public void testExclusiveOverridesArePreserved() throws IOException {
    Assert.assertEquals(mapper.readValue(clusterThreeFinal, type), topics(conf, "cluster_three"));
  }

  @Test
  public void testDefaultValues() throws Exception {
    List<Map<String, Object>> expected = mapper.readValue(String.join("\n",
            "---",
            "- name: topic_b",
            "  partitions: 1",
            "  replicationFactor: 1",
            "  config: ",
            "    k1: v1",
            "    k2: v2"), type);
    Assert.assertEquals(expected,
        topics(conf, "cluster_three", ImmutableMap.of(
            "partitions",  "2",
            "config", ImmutableMap.of(
                "k2", "v2"
            )
        )));
  }

  @Test
  public void testDefaultClusterValues() throws Exception {
    List<Map<String, Object>> expected = mapper.readValue(String.join("\n",
        "---",
        "- name: topic_b",
        "  partitions: 1",
        "  replicationFactor: 1",
        "  config: ",
        "    k1: v1",
        "    k2: v2"), type);
    Assert.assertEquals(expected, topics(conf, "cluster_three", mapper.readValue(String.join("\n",
            "---",
            "config:",
            "  k2: v1",
            "clusters:",
            "  cluster_three:",
            "     config:",
            "       k2: v2"), new TypeReference<Map<String, Object>>(){})));
  }

  @Test
  public void testMapPropertiesAreOverridden() throws IOException {
    String conf = String.join(
        "\n",
        new String[]{
            "---",
            "- name: topic_a",
            "  partitions: 1",
            "  replicationFactor: 1",
            "  config: ",
            "    k1: v1",
            "    k2: v2",
            "  clusters:",
            "    cluster: ",
            "      config: ",
            "        k2: v2.1",
        });
    String clusterFinal = String.join(
        "\n",
        new String[]{
            "---",
            "- name: topic_a",
            "  partitions: 1",
            "  replicationFactor: 1",
            "  config: ",
            "    k1: v1",
            "    k2: v2.1"
        });
    Assert.assertEquals(mapper.readValue(clusterFinal, type), topics(conf, "cluster"));
  }

}
