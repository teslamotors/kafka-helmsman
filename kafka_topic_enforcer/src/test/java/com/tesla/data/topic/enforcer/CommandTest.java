/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import com.tesla.data.topic.enforcer.BaseCommand.CommandConfigConverter;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

public class CommandTest {

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

  private CommandConfigConverter converter = new CommandConfigConverter();

  private static InputStream confStream(String confStr) {
    return new ByteArrayInputStream(confStr.getBytes(Charset.forName("UTF-8")));
  }

  @Test
  public void tesConverter() throws IOException {
    Map<String, Object> config = converter.convert(confStream(testConf));
    Assert.assertTrue(config.containsKey("kafka"));
    Assert.assertTrue(config.containsKey("topics"));
  }

  @Test
  public void testConfiguredTopics() throws IOException {
    Map<String, Object> config = converter.convert(confStream(testConf));
    List<ConfiguredTopic> configuredTopics = new BaseCommand(config).configuredTopics();
    Assert.assertEquals(2, configuredTopics.size());
    Assert.assertEquals("topic_a", configuredTopics.get(0).getName());
    Assert.assertEquals(1, configuredTopics.get(0).getPartitions());
    Assert.assertEquals(1, configuredTopics.get(0).getReplicationFactor());
    Assert.assertEquals("gzip", configuredTopics.get(0).getConfig().get("compression.type"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfiguredTopicsBad() throws IOException {
    String badConf = "---\n" +
        "kafka:\n" +
        "  bootstrap.servers: localhost:9092\n" +
        "topics:\n" +
        "  - name: topic_a    \n" +
        "    replicationFactor: 1";
    Map<String, Object> config = converter.convert(confStream(badConf));
    new BaseCommand(config).configuredTopics();
  }

}