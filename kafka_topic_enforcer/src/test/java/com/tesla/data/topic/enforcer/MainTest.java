/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class MainTest {

  private String testConf = "---\n" +
      "kafka:\n" +
      "    bootstrap.servers: localhost:9092\n" +
      "\n" +
      "topics:\n" +
      "    - name: topic_a\n" +
      "      partitions: 1\n" +
      "      replicationFactor: 1\n" +
      "      config:\n" +
      "          retention.ms: 604800000\n" +
      "          compression.type: gzip\n" +
      "    - name: topic_b\n" +
      "      partitions: 1\n" +
      "      replicationFactor: 1";


  private static Map<String, Object> loadConfig(String confStr) throws IOException {
    return Main.loadConfig(new ByteArrayInputStream(confStr.getBytes(Charset.forName("UTF-8"))));
  }

  @Test
  public void testLoadConfig() throws IOException {
    Map<String, Object> config = loadConfig(testConf);
    Assert.assertTrue(config.containsKey("kafka"));
    Assert.assertTrue(config.containsKey("topics"));
  }

  @Test
  public void testTopicsFromConf() throws IOException {
    List<ConfiguredTopic> configuredTopics = Main.topicsFromConf(loadConfig(testConf));
    Assert.assertEquals(2, configuredTopics.size());
    Assert.assertEquals("topic_a", configuredTopics.get(0).getName());
    Assert.assertEquals(1, configuredTopics.get(0).getPartitions());
    Assert.assertEquals(1, configuredTopics.get(0).getReplicationFactor());
    Assert.assertEquals("gzip", configuredTopics.get(0).getConfig().get("compression.type"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testTopicFromConfBad() throws IOException {
    String badConf = "---\n" +
        "kafka:\n" +
        "  bootstrap.servers: localhost:9092\n" +
        "topics:\n" +
        "  - name: topic_a    \n" +
        "    replicationFactor: 1";
    Main.topicsFromConf(loadConfig(badConf));
  }

}