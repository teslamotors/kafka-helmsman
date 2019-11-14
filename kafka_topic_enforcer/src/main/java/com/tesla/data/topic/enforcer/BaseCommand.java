/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * All topic enforcement related CLI tools are an extension of {@link BaseCommand}.
 */
@Parameters(commandDescription = "Validate config")
class BaseCommand {

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

  final Logger LOG = LoggerFactory.getLogger(getClass());

  BaseCommand() {
    // DO NOT REMOVE, this is needed by jcommander
  }

  // for testing
  BaseCommand(Map<String, Object> cmdConfig) {
    this.cmdConfig = cmdConfig;
  }

  Map<String, Object> cmdConfig() {
    Objects.requireNonNull(cmdConfig, "command has not been initialized with config");
    return cmdConfig;
  }

  Map<String, Object> kafkaConfig() {
    return MAPPER.convertValue(cmdConfig().get("kafka"), MAP_TYPE);
  }

  List<ConfiguredTopic> configuredTopics() {
    return Arrays.asList(MAPPER.convertValue(cmdConfig().get("topics"), ConfiguredTopic[].class));
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
  static class CommandConfigConverter implements IStringConverter<Map<String, Object>> {
    @Override
    public Map<String, Object> convert(String file) {
      try {
        return convert(new FileInputStream(file));
      } catch (IOException e) {
        throw new ParameterException("Could not load config from file " + file, e);
      }
    }

    Map<String, Object> convert(InputStream is) throws IOException {
      Map<String, Object> cmdConfig = MAPPER.readValue(is, MAP_TYPE);
      Objects.requireNonNull(cmdConfig.get("topics"), "Missing topics from config");
      Objects.requireNonNull(cmdConfig.get("kafka"), "Missing kafka connection settings from config");
      return cmdConfig;
    }
  }

}
