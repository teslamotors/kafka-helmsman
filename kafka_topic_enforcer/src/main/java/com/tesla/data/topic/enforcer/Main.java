/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The app.
 */
public class Main {

  private static final Logger LOG = LoggerFactory.getLogger(Enforcer.class);
  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<Map<String, Object>>() {
  };
  private static final String DUMP_CMD = "dump";
  private static final String ENFORCE_CMD = "enforce";
  private static final Gauge lastSuccess = Gauge.build()
      .name("kafka_topic_enforcer_run_last_success_timestamp")
      .help("Timestamp of last successful run as epoch seconds.")
      .register();
  private static final Counter failures = Counter.build()
      .name("kafka_topic_enforcer_run_failures_total")
      .help("Total failures so far.")
      .register();
  private static final Counter runs = Counter.build()
      .name("kafka_topic_enforcer_run_total")
      .help("Total runs so far.")
      .register();


  // Setups jackson magic that uses parameter name information provided by the Java Reflection API,
  // this lets us define pojos with final fields without jackson property annotation.
  static {
    MAPPER.registerModule(new ParameterNamesModule());
  }

  @Parameter(
      names = {"--help", "-h"},
      help = true)
  private boolean help = false;

  private static class CommonCommandParams {
    @Parameter(
        description = "/path/to/a/configuration/file",
        required = true)
    private String configFile = null;
  }

  @Parameters(commandDescription = "Enforce given configuration")
  private static class EnforceCommand {
    @Parameter(
        names = {"--dryrun", "-d"},
        description = "do a dry run")
    private boolean dryrun = false;

    @Parameter(
        names = {"--unsafemode"},
        description = "run in unsafe mode, topic deletion is _only_ allowed in this mode")
    private boolean unsafemode = false;

    @Parameter(
        names = {"--continuous", "-c"},
        description = "run enforcement continuously")
    private boolean continuous = false;

    @Parameter(
        names = {"--interval", "-i"},
        description = "run interval for continuous mode in seconds",
        arity = 1)
    private int interval = 600;

    @ParametersDelegate
    private CommonCommandParams common = new CommonCommandParams();
  }

  @Parameters(commandDescription = "Dump existing cluster config on stdout")
  private static class DumpCommand {
    @ParametersDelegate
    private CommonCommandParams common = new CommonCommandParams();
  }

  private final DumpCommand dumpCmd = new DumpCommand();
  private final EnforceCommand enforceCmd = new EnforceCommand();

  public static void main(String[] args) throws Exception {
    Main main = new Main();
    JCommander commander = JCommander.newBuilder()
        .addObject(main)
        .addCommand(DUMP_CMD, main.dumpCmd)
        .addCommand(ENFORCE_CMD, main.enforceCmd)
        .build();
    commander.parse(args);

    if (main.help || args.length == 0) {
      commander.usage();
      return;
    }

    String configFile = main.dumpCmd.common.configFile != null ?
            main.dumpCmd.common.configFile : main.enforceCmd.common.configFile;
    Map<String, Object> config = loadConfig(new FileInputStream(new File(configFile)));
    Map<String, Object> kafkaProperties = MAPPER.convertValue(config.get("kafka"), MAP_TYPE);
    try (AdminClient adminClient = KafkaAdminClient.create(kafkaProperties)) {
      main.run(commander.getParsedCommand(), adminClient, config);
    }
  }

  void run(String command, AdminClient kafka, Map<String, Object> config) throws Exception {
    TopicService topicService;
    switch (command) {
      case DUMP_CMD:
        topicService = new TopicServiceImpl(kafka, true);
        Collection<ConfiguredTopic> existing = topicService.listExisting(true).values()
            .stream()
            .sorted(Comparator.comparing(ConfiguredTopic::getName))
            .collect(Collectors.toList());
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(existing));
        break;
      case ENFORCE_CMD:
        LOG.info("Dry run is {}, safe mode is {}",
            enforceCmd.dryrun ? "ON" : "OFF", enforceCmd.unsafemode ? "OFF" : "ON");
        topicService = new TopicServiceImpl(kafka, enforceCmd.dryrun);
        Enforcer enforcer = new Enforcer(topicService, topicsFromConf(config), !enforceCmd.unsafemode);
        if (!enforceCmd.continuous) {
          enforcer.enforceAll();
        } else {
          HTTPServer metricServer = null;
          try {
            int port = (int) config.getOrDefault("metricsPort", 8081);
            metricServer = new HTTPServer(port);
            LOG.info("Started metrics server at port {}", port);
            while (true) {
              // Stats should be recorded before enforcement or else we will miss all anomalies
              runs.inc();
              enforcer.stats();
              enforcer.enforceAll();
              lastSuccess.setToCurrentTime();
              Thread.sleep(enforceCmd.interval * 1000);
            }
          } catch (Exception e) {
            LOG.info("Enforcer run failed!", e);
            failures.inc();
          } finally {
            if (metricServer != null) {
              metricServer.stop();
            }
          }
        }
        break;

      default:
        throw new IllegalStateException("Unknown command: " + command);
    }
  }

  static List<ConfiguredTopic> topicsFromConf(Map<String, Object> config) {
    return Arrays.asList(MAPPER.convertValue(config.get("topics"), ConfiguredTopic[].class));
  }

  static Map<String, Object> loadConfig(InputStream is) throws IOException {
    Map<String, Object> config = MAPPER.readValue(is, MAP_TYPE);
    Objects.requireNonNull(config.get("topics"), "Missing topics from config");
    Objects.requireNonNull(config.get("kafka"), "Missing kafka connection settings from config");
    return config;
  }

}
