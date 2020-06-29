/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import com.tesla.data.topic.enforcer.BaseCommand.CommandConfigConverter;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CommandTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private String testConf =
      "---\n"
          + "kafka:\n"
          + "    bootstrap.servers: localhost:9092\n"
          + "\n"
          + "topics:\n"
          + "    - name: topic_a\n"
          + "      partitions: 1\n"
          + "      replicationFactor: 1\n"
          + "      config:\n"
          + "          retention.ms: 604800000\n"
          + "          compression.type: gzip\n"
          + "      tags:\n"
          + "          owner: team@company.com\n"
          + "          datasets:\n"
          + "             - root.domain.product.data.type-one\n"
          + "             - root.domain.product.data.type-two\n"
          + "    - name: topic_b\n"
          + "      partitions: 1\n"
          + "      replicationFactor: 1";

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
    Assert.assertEquals("team@company.com", configuredTopics.get(0).getTags().get("owner"));
    Assert.assertEquals(
        Arrays.asList("root.domain.product.data.type-one", "root.domain.product.data.type-two"),
        configuredTopics.get(0).getTags().get("datasets"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfiguredTopicsBad() throws IOException {
    String badConf =
        "---\n"
            + "kafka:\n"
            + "  bootstrap.servers: localhost:9092\n"
            + "topics:\n"
            + "  - name: topic_a    \n"
            + "    replicationFactor: 1";
    Map<String, Object> config = converter.convert(confStream(badConf));
    new BaseCommand(config).configuredTopics();
  }

  private Map<String, Object> configWithTopicsFile(String topics) throws IOException {
    File tempFile = TEMP_FOLDER.newFile();
    Files.write(Paths.get(tempFile.getPath()), topics.getBytes());

    String testConf =
        String.join(
            "\n",
            new String[]{
                "---",
                "kafka:",
                "  bootstrap.servers: localhost:9092",
                "topicsFile: " + tempFile.getPath()
            });

    return converter.convert(confStream(testConf));
  }

  @Test
  public void testTopicsFromFile() throws IOException {
    String topics =
        String.join(
            "\n",
            new String[]{
                "---",
                "- name: topic_a",
                "  partitions: 1",
                "  replicationFactor: 1",
            });
    Assert.assertEquals(1, new BaseCommand(configWithTopicsFile(topics)).configuredTopics().size());
  }

  @Test
  public void testClusterTopicsFromFile() throws IOException {
    String topics =
        String.join(
            "\n",
            new String[]{
                "---",
                "- name: topic_a",
                "  partitions: 1",
                "  replicationFactor: 1",
                "  clusters:",
                "    cluster_a: {}",
            });
    Assert.assertEquals(
        1, new BaseCommand(configWithTopicsFile(topics), "cluster_a").configuredTopics().size());
    Assert.assertEquals(
        0, new BaseCommand(configWithTopicsFile(topics), "cluster_b").configuredTopics().size());
  }
}
