/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static com.tesla.data.topic.enforcer.ClusterTopics.topicConfigsForCluster;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * All topic enforcement related CLI tools are an extension of {@link BaseCommand}.
 */
@Parameters(commandDescription = "Validate config")
public class BaseCommand {

  static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
  };
  static int SUCCESS = 0;
  static int FAILURE = 1;

  static {
    MAPPER.registerModule(new ParameterNamesModule());
  }

  @Parameter(
      description = "/path/to/a/configuration/file",
      required = true,
      converter = CommandConfigConverter.class)
  private Map<String, Object> cmdConfig = null;

  @Parameter(
      names = {"--cluster"},
      description = "a cluster name, if specified, " +
          "consolidated (multi-cluster) topic configuration file is expected")
  private String cluster = null;

  final Logger LOG = LoggerFactory.getLogger(getClass());

  BaseCommand() {
    // DO NOT REMOVE, this is needed by jcommander
  }

  public BaseCommand(Map<String, Object> cmdConfig) {
    this(cmdConfig, null);
  }

  public BaseCommand(Map<String, Object> cmdConfig, String cluster) {
    this.cmdConfig = cmdConfig;
    this.cluster = cluster;
  }

  Map<String, Object> cmdConfig() {
    Objects.requireNonNull(cmdConfig, "command has not been initialized with config");
    return cmdConfig;
  }

  public Map<String, Object> kafkaConfig() {
    return MAPPER.convertValue(cmdConfig().get("kafka"), MAP_TYPE);
  }

  public List<ConfiguredTopic> configuredTopics() {
    List<Map<String, Object>> unParsed = configuredTopics(cmdConfig());
    List<Map<String, Object>> forCluster =
        cluster == null ? unParsed : topicConfigsForCluster(unParsed, cluster);
    return forCluster.stream()
        .map(t -> MAPPER.convertValue(t, ConfiguredTopic.class))
        .collect(Collectors.toList());
  }

  // un-parsed list of configured topics
  private List<Map<String, Object>> configuredTopics(Map<String, Object> cmdConfig) {
    // both 'topicsFile' & 'topics' YAMLs are a list of maps
    TypeReference<List<Map<String, Object>>> listOfMaps =
        new TypeReference<List<Map<String, Object>>>() {};
    if (cmdConfig.containsKey("topics")) {
      return MAPPER.convertValue(cmdConfig.get("topics"), listOfMaps);
    } else {
      try {
        return MAPPER.readValue(
            new FileInputStream((String) cmdConfig.get("topicsFile")), listOfMaps);
      } catch (IOException e) {
        throw new ParameterException(
            "Could not load topics from file " + cmdConfig.get("topicsFile"), e);
      }
    }
  }

  public int run() {
    System.out.println("Kafka connection: " + kafkaConfig().get("bootstrap.servers"));
    System.out.println("Configured topic count: " + configuredTopics().size());
    System.out.println("Config looks good!");
    return SUCCESS;
  }

  /**
   * An converter that converts yaml config file to a config stored in a java map.
   */
  public static class CommandConfigConverter implements IStringConverter<Map<String, Object>> {
    @Override
    public Map<String, Object> convert(String file) {
      try {
        return convert(new FileInputStream(file));
      } catch (IOException e) {
        throw new ParameterException("Could not load config from file " + file, e);
      }
    }

    public Map<String, Object> convert(InputStream is) throws IOException {
      Map<String, Object> cmdConfig = MAPPER.readValue(is, MAP_TYPE);
      Object topics = cmdConfig.containsKey("topics") ? cmdConfig.get("topics") : cmdConfig.get("topicsFile");
      Objects.requireNonNull(topics, "Missing topics from config");
      Objects.requireNonNull(cmdConfig.get("kafka"), "Missing kafka connection settings from config");
      return cmdConfig;
    }
  }

}
