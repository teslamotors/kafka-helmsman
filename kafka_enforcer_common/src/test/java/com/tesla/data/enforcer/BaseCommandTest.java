/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import com.tesla.data.enforcer.BaseCommand.CommandConfigConverter;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import tesla.shade.com.google.common.collect.ImmutableMap;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public class BaseCommandTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();
  private static final String testConf =
      "---\n"
          + "kafka:\n"
          + "    bootstrap.servers: localhost:9092\n"
          + "\n"
          + "dummies:\n"
          + "    - name: dummy_a\n"
          + "      config:\n"
          + "          k1: v1\n"
          + "          k2: v2\n"
          + "    - name: dummy_b\n"
          + "      config: {}";
  private final CommandConfigConverter converter = new CommandConfigConverter();

  private static InputStream confStream(String confStr) {
    return new ByteArrayInputStream(confStr.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testConverter() throws IOException {
    Map<String, Object> config = converter.convert(confStream(testConf));
    Assert.assertTrue(config.containsKey("kafka"));
    Assert.assertTrue(config.containsKey("dummies"));
  }

  @Test
  public void testConfiguredT() throws IOException {
    Map<String, Object> config = converter.convert(confStream(testConf));
    List<Dummy> configured =
        new BaseCommand<Dummy>(config).configuredEntities(Dummy.class, "dummies", "dummiesFile");
    Assert.assertEquals(2, configured.size());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testConfiguredBad() throws IOException {
    String badConf =
        "---\n"
            + "kafka:\n"
            + "  bootstrap.servers: localhost:9092\n"
            + "dummies:\n"
            + "  - name: dummy_a\n";
    Map<String, Object> config = converter.convert(confStream(badConf));
    new BaseCommand<Dummy>(config).configuredEntities(Dummy.class, "dummies", "dummiesFile");
  }

  private Map<String, Object> configWithEntitiesFile(String data) throws IOException {
    File tempFile = TEMP_FOLDER.newFile();
    Files.write(Paths.get(tempFile.getPath()), data.getBytes());

    String testConf =
        String.join(
            "\n",
            new String[]{
                "---",
                "kafka:",
                "  bootstrap.servers: localhost:9092",
                "dummiesFile: " + tempFile.getPath()
            });

    return converter.convert(confStream(testConf));
  }

  @Test
  public void testFromFile() throws IOException {
    String dummies =
        String.join(
            "\n",
            new String[]{
                "---",
                "- name: dummy_a",
                "  config: {}"
            });
    Assert.assertEquals(
        1,
        new BaseCommand<Dummy>(configWithEntitiesFile(dummies))
            .configuredEntities(Dummy.class, "dummies", "dummiesFile")
            .size());
  }

  @Test
  public void testClusterEntitiesFromFile() throws IOException {
    String dummies =
        String.join(
            "\n",
            new String[]{
                "---",
                "- name: dummy_a",
                "  config: {}",
                "  clusters:",
                "    cluster_a: {}",
            });
    Assert.assertEquals(
        1,
        new BaseCommand<Dummy>(configWithEntitiesFile(dummies), "cluster_a")
            .configuredEntities(Dummy.class, "dummies", "dummiesFile")
            .size());
    Assert.assertEquals(
        0,
        new BaseCommand<Dummy>(configWithEntitiesFile(dummies), "cluster_b")
            .configuredEntities(Dummy.class, "dummies", "dummiesFile")
            .size());
  }

  @Test
  public void testEntitesWithDefaults() throws Exception {
    String dummies = String.join("\n",
        "---",
        "kafka:",
        "    bootstrap.servers: localhost:9092",
        "dummies:",
        "    - name: dummy_a",
        "      config:",
        "          k1: v1",
        "          k2: v2",
        "    - name: dummy_b",
        "      config: {}",
        "defaults:",
        "  config:",
        "    k1: v_default",
        "    k3: v3");
    Map<String, Object> config = converter.convert(confStream(dummies));
    List<Dummy> configured =
        new BaseCommand<Dummy>(config).configuredEntities(Dummy.class, "dummies", "dummiesFile");
    Assert.assertEquals(2, configured.size());
    Assert.assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"), configured.get(0).config);
    Assert.assertEquals(ImmutableMap.of("k1", "v_default", "k3", "v3"), configured.get(1).config);
  }

  @Test
  public void testClusterEntitiesWithDefaults() throws Exception {
    String dummies = String.join("\n",
        "---",
        "kafka:",
        "    bootstrap.servers: localhost:9092",
        "dummies:",
        "    - name: dummy_a",
        "      config:",
        "          k1: v1",
        "          k2: v2",
        "      clusters:",
        "        cluster_a: {}",
        "        cluster_b:",
        "          config:",
        "            k3: b3",
        "    - name: dummy_b",
        "      config: {}",
        "      clusters:",
        "        cluster_b:",
        "          config:",
        "            k3: b3",
        "defaults:",
        "    config:",
        "        k1: v_default",
        "        k3: v3",
        "    clusters:",
        "        cluster_b:",
        "            config:",
        "                k2: d2"
    );
    Map<String, Object> config = converter.convert(confStream(dummies));
    List<Dummy> clusterA =
        new BaseCommand<Dummy>(config, "cluster_a")
            .configuredEntities(Dummy.class, "dummies", "dummiesFile");
    Assert.assertEquals(1, clusterA.size());
    Assert.assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "v3"), clusterA.get(0).config);

    List<Dummy> clusterB =
        new BaseCommand<Dummy>(config, "cluster_b")
            .configuredEntities(Dummy.class, "dummies", "dummiesFile");
    Assert.assertEquals(2, clusterB.size());
    Assert.assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2", "k3", "b3"), clusterB.get(0).config);
    Assert.assertEquals(ImmutableMap.of("k1", "v_default", "k2", "d2", "k3", "b3"), clusterB.get(1).config);
  }
}
