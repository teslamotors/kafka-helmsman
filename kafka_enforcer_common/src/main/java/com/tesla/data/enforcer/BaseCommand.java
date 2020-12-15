/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import static java.lang.String.format;

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
import tesla.shade.com.google.common.io.Resources;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * All enforcement related CLI tools are an extension of {@link BaseCommand}.
 */
@Parameters(commandDescription = "Validate config")
public class BaseCommand<T> {

  protected static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
  };
  public static int SUCCESS = 0;
  public static int FAILURE = 1;

  static {
    MAPPER.registerModule(new ParameterNamesModule());
  }

  protected final Logger LOG = LoggerFactory.getLogger(getClass());
  @Parameter(
      description = "/path/to/a/configuration/file",
      required = true,
      converter = CommandConfigConverter.class)
  protected Map<String, Object> cmdConfig = null;

  @Parameter(
      names = {"--cluster"},
      description = "a cluster name, if specified, " +
          "consolidated (multi-cluster) configuration file is expected")
  protected String cluster = null;

  public BaseCommand() {
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

  public List<T> configuredEntities(Class<T> toValueType, String entitiesKey, String entitiesFileKey) {
    LOG.info("Config contains, {}: {}, {}: {}",
        entitiesKey, cmdConfig.containsKey(entitiesKey), entitiesFileKey, cmdConfig.containsKey(entitiesFileKey));
    Object entities = cmdConfig.containsKey(entitiesKey) ? cmdConfig.get(entitiesKey) : cmdConfig.get(entitiesFileKey);
    Objects.requireNonNull(entities, "Missing entities in config");
    Optional<Map<String, Object>> defaults = configuredEntities(cmdConfig, "defaults", null, MAP_TYPE);
    List<Map<String, Object>> unParsed = configuredEntities(cmdConfig, entitiesKey, entitiesFileKey);
    final List<Map<String, Object>> forCluster;
    if (cluster == null) {
      // if there are defaults, use those as the basis, otherwise just use the discovered entities
      forCluster = defaults.map(defaultValues ->
          unParsed.stream().map(entity -> {
            Map<String, Object> updated = new HashMap<>(defaultValues);
            return ClusterEntities.applyOverrides(updated, entity);
          }).collect(Collectors.toList())).orElse(unParsed);
      LOG.info("Cluster is not set");
    } else {
      forCluster = ClusterEntities.forCluster(unParsed, cluster, defaults);
      LOG.info("Out of total {}, found {} matching entities for cluster {}", unParsed.size(), forCluster.size(), cluster);
    }
    return forCluster.stream()
        .map(t -> MAPPER.convertValue(t, toValueType))
        .collect(Collectors.toList());
  }

  // un-parsed list of configured entities
  private List<Map<String, Object>> configuredEntities(Map<String, Object> cmdConfig, String entitiesKey, String entitiesFileKey) {
    TypeReference<List<Map<String, Object>>> listOfMaps =
        new TypeReference<List<Map<String, Object>>>() {
        };
    return configuredEntities(cmdConfig, entitiesKey, entitiesFileKey, listOfMaps).orElseThrow(
        () -> new IllegalArgumentException(format("Missing entities at %s or %s", entitiesKey, entitiesFileKey)));
  }


  private <ENTITY> Optional<ENTITY> configuredEntities(Map<String, Object> cmdConfig, String entitiesKey,
                                                       String entitiesFileKey, TypeReference<ENTITY> entity) {
    if (cmdConfig.containsKey(entitiesKey)) {
      return Optional.of(MAPPER.convertValue(cmdConfig.get(entitiesKey), entity));
    }
    if (entitiesFileKey == null) {
      return Optional.empty();
    } else {
      try {
        String entitiesFile = (String) cmdConfig.get(entitiesFileKey);
        final InputStream is;
        if (Files.exists(Paths.get(entitiesFile))) {
          is = new FileInputStream(entitiesFile);
        } else {
          is = Resources.getResource(entitiesFile).openStream();
        }
        return Optional.of(MAPPER.readValue(is, entity));
      } catch (IOException e) {
        throw new ParameterException(
            "Could not load entities from file " + cmdConfig.get(entitiesFileKey), e);
      }
    }
  }

  public int run() {
    System.out.println("Kafka connection: " + kafkaConfig().get("bootstrap.servers"));
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
      Objects.requireNonNull(cmdConfig.get("kafka"), "Missing kafka connection settings from config");
      return cmdConfig;
    }
  }
}
