/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import static tesla.shade.com.google.common.base.Preconditions.checkNotNull;
import static tesla.shade.com.google.common.base.Preconditions.checkState;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

@Parameters(commandDescription = "Enforce given configuration")
public class EnforceCommand<T> extends BaseCommand<T> {

  private static final Gauge lastSuccess = Gauge.build()
      .name("kafka_enforcer_run_last_success_timestamp")
      .help("Timestamp of last successful run as epoch seconds.")
      .register();
  private static final Counter failures = Counter.build()
      .name("kafka_enforce_run_failures_total")
      .help("Total failures so far.")
      .register();
  private static final Counter runs = Counter.build()
      .name("kafka_enforcer_run_total")
      .help("Total runs so far.")
      .register();

  @Parameter(
      names = {"--dryrun", "-d"},
      description = "do a dry run")
  protected boolean dryrun = false;
  @Parameter(
      names = {"--unsafemode"},
      description = "run in unsafe mode, deletion is _only_ allowed in this mode")
  protected boolean unsafemode = false;
  @Parameter(
      names = {"--continuous", "-c"},
      description = "run enforcement continuously")
  protected boolean continuous = false;
  @Parameter(
      names = {"--interval", "-i"},
      description = "run interval for continuous mode in seconds",
      arity = 1)
  protected int interval = 600;

  private Enforcer<T> enforcer;

  EnforceCommand() {
    // DO NOT REMOVE, this is needed by jcommander
  }

  // This constructor is only meant for testing. In a non-testing/real scenario, the subclass must override
  // initEnforcer and initialize the enforcer.
  EnforceCommand(Enforcer<T> enforcer, Map<String, Object> cmdConfig, boolean continuous, int interval) {
    super(cmdConfig);
    this.enforcer = enforcer;
    this.continuous = continuous;
    this.interval = interval;
  }

  /**
   * Initialize the enforcer. Base classes must override this method.
   *
   * @param adminClient a kafka admin client
   * @return an enforcer instance
   */
  protected Enforcer<T> initEnforcer(AdminClient adminClient) {
    return checkNotNull(enforcer, "Enforcer is expected to be initialized when command instance is created " +
        "for a test.");
  }

  @Override
  public int run() {
    try (AdminClient kafka = KafkaAdminClient.create(kafkaConfig())) {
      checkState(enforcer == null, "Enforcer is expected to be un-initialized when command " +
          "instance is created by CLI");
      this.enforcer = initEnforcer(kafka);
      return doRun(() -> false);
    }
  }

  // for testing
  int doRun(final Supplier<Boolean> stopCondition) {
    LOG.info("Dry run is {}, safe mode is {}", dryrun ? "ON" : "OFF", unsafemode ? "OFF" : "ON");
    if (!continuous) {
      return runOnceIgnoreError();
    }

    // continuous mode
    HTTPServer server = null;
    final int port = (int) cmdConfig().getOrDefault("metricsPort", 8081);
    try {
      server = metricServer(port);
      LOG.info("Started metrics server at port {}", port);
      while (!stopCondition.get()) {
        runOnceIgnoreError();
        Thread.sleep(interval * 1000);
      }
    } catch (Exception e) {
      // exception caught here must have come prometheus server or thread interrupt
      // all other exceptions are swallowed by runOnceIgnoreError routine
      LOG.error("Unexpected error", e);
      return FAILURE;
    } finally {
      if (server != null) {
        server.stop();
      }
    }
    return SUCCESS;
  }

  // for testing
  HTTPServer metricServer(int port) throws IOException {
    return new HTTPServer(port, true);
  }

  private int runOnceIgnoreError() {
    try {
      runs.inc();
      // Stats should be recorded before enforcement or else we will miss all anomalies
      this.enforcer.stats();
      this.enforcer.enforceAll();
      lastSuccess.setToCurrentTime();
      return SUCCESS;
    } catch (Exception e) {
      LOG.error("Enforcer run failed!", e);
      failures.inc();
      return FAILURE;
    }
  }

}
