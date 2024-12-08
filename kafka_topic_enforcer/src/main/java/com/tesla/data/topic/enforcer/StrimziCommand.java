/*
 * Copyright (C) 2024 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import com.tesla.data.enforcer.BaseCommand;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.common.base.CaseFormat;
import com.google.common.hash.Hashing;
import io.strimzi.api.kafka.model.topic.KafkaTopic;
import io.strimzi.api.kafka.model.topic.KafkaTopicBuilder;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Parameters(commandDescription = "Generate the Strimzi KafkaTopic resources YAML " +
    "(https://strimzi.io/docs/operators/latest/configuring.html#type-KafkaTopic-reference) " +
    "on stdout from the topic config. Certain information such as topic tags and config comments would be missed, " +
    "and the resource metadata names are also converted to conform RFC 1123.")
public class StrimziCommand extends BaseCommand<ConfiguredTopic> {

  @Parameter(
      names = {"--kafka_cluster", "-k"},
      description = "the name of the Strimzi Kafka cluster to be generated for",
      required = true)
  protected String kafkaCluster;

  @Override
  public int run() {
    List<ConfiguredTopic> configuredTopics = configuredEntities(ConfiguredTopic.class, "topics", "topicsFile");

    ObjectMapper yamlMapper = new ObjectMapper(new YAMLFactory()
        .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, true)
        .configure(YAMLGenerator.Feature.LITERAL_BLOCK_STYLE, true)
        .configure(YAMLGenerator.Feature.MINIMIZE_QUOTES, true)
        .configure(YAMLGenerator.Feature.INDENT_ARRAYS, false)
        .configure(YAMLGenerator.Feature.SPLIT_LINES, false)
    );

    try {
      for (ConfiguredTopic topic : configuredTopics) {
        System.out.println(yamlMapper.writeValueAsString(getKafkaTopic(topic)));
      }
    } catch (JsonProcessingException e) {
      LOG.error("Failed to dump config", e);
      return FAILURE;
    }

    return SUCCESS;
  }

  /**
   * Takes the best effort to convert the topic names to a resource name compatible format. We are not aiming at perfect
   * correctness for mixed cases. Instead, we take some heuristics to get some good enough results.
   */
  private String toResourceName(String s) {
    // If the topic name contains an underscore, we assume it follows underscore case.
    if (s.contains("_")) {
      return CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, s.toLowerCase()).replaceAll("[^-.a-z0-9]","");
    }
    // We assume the original name is upper camel case and take the best effort, even if it's not.
    return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, s).replaceAll("[^-.a-z0-9]","");
  }

  private KafkaTopic getKafkaTopic(ConfiguredTopic enforcerTopic) {
    // Kubernetes resources follow RFC 1123 naming convention, so we have to remove the unsupported characters. To avoid
    // duplications, we are appending a short hash string of the original topic name.
    String topicHash = Hashing.sha256().hashString(enforcerTopic.getName(), StandardCharsets.UTF_8)
        .toString().substring(0, 6);
    // There could be multiple topics with the same name but belonging to different Kafka clusters. They are different
    // topics. To avoid ambiguity, we prepend the Kafka cluster names in the KafkaTopic metadata name.
    String resourceName = kafkaCluster + "." + toResourceName(enforcerTopic.getName()) + "-" + topicHash;

    return new KafkaTopicBuilder()
        .withNewMetadata()
          .withName(resourceName)
          .withLabels(Map.of("strimzi.io/cluster", kafkaCluster))
        .endMetadata()
        .withNewSpec()
          .withTopicName(enforcerTopic.getName())
          .withPartitions(enforcerTopic.getPartitions())
          .withReplicas((int) enforcerTopic.getReplicationFactor())
          .withConfig(new HashMap<>(enforcerTopic.getConfig()))
        .endSpec().build();
  }
}


