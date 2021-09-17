/*
 * Copyright © 2018 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import static java.lang.System.exit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tesla.shade.com.google.common.annotations.VisibleForTesting;
import tesla.shade.com.google.common.base.Preconditions;
import tesla.shade.com.google.common.util.concurrent.FutureCallback;
import tesla.shade.com.google.common.util.concurrent.Futures;
import tesla.shade.com.google.common.util.concurrent.ListenableFuture;
import tesla.shade.com.google.common.util.concurrent.ListeningExecutorService;
import tesla.shade.com.google.common.util.concurrent.MoreExecutors;

import java.io.File;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.stream.Collectors;
import java.util.HashSet;

public class ConsumerFreshness {

  private static final Logger LOG = LoggerFactory.getLogger(ConsumerFreshness.class);
  private static final int DEFAULT_WORKER_THREADS = 15;
  private static final int DEFAULT_KAFKA_CONSUMER_COUNT = 15;

  @Parameter(names = "--conf", description = "Path to configuration to run")
  private String conf = null;

  @Parameter(names = "--once", description = "Run loop once")
  private boolean once = false;

  @Parameter(names = "--help", help = true)
  private boolean help = false;

  @Parameter(names = "--strict", description = "Run in strict configuration validation mode")
  boolean strict = false;

  private FreshnessMetrics metrics = new FreshnessMetrics();
  private Burrow burrow;
  private Map<String, ArrayBlockingQueue<KafkaConsumer>> availableWorkers;
  private ListeningExecutorService executor;

  public static void main(String[] args) throws IOException, InterruptedException {
    ConsumerFreshness freshness = new ConsumerFreshness();
    JCommander command = JCommander.newBuilder()
        .addObject(freshness)
        .build();
    command.parse(args);
    if (freshness.help) {
      command.usage();
      exit(0);
    }

    // initialize JVM metrics
    DefaultExports.initialize();

    ObjectMapper confMapper = new ObjectMapper(new YAMLFactory());
    Map<String, Object> conf = confMapper.readValue(new File(freshness.conf), Map.class);

    HTTPServer server = null;
    try {
      int port = (Integer) conf.getOrDefault("port", 8081);
      server = new HTTPServer(port);
      freshness.setup(conf);

      // run once and dump the local server state
      if (freshness.once) {
        freshness.run();
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
            .url(String.format("http://localhost:%s/metrics", port))
            .get().build();
        try (Response response = client.newCall(request).execute()) {
          LOG.info(response.body().string());
        }
        return;
      }

      // run forever
      int frequency = (int) conf.getOrDefault("frequency_sec", 30);
      while (true) {
        freshness.run();
        Thread.sleep(frequency * 1000);
      }
    } finally {
      freshness.stop();
      if (server != null) {
        server.stop();
      }
    }
  }

  private void setup(Map<String, Object> conf) {
    setupWithBurrow(conf, new Burrow((Map<String, Object>) conf.get("burrow")));
  }

  @VisibleForTesting
  void setupWithBurrow(Map<String, Object> conf, Burrow burrow) {
    this.burrow = burrow;
    int workerThreadCount = (int) conf.getOrDefault("workerThreadCount", DEFAULT_WORKER_THREADS);
    this.availableWorkers = ((List<Map<String, Object>>) conf.get("clusters")).stream()
        .map(clusterConf -> {
          // validate the cluster configuration
          Optional<String> validationErrorMsg = validateClusterConf(clusterConf);
          if (validationErrorMsg.isPresent()) {
            String msg = String.format("configuration for cluster %s is invalid: %s",
                    clusterConf.get("name"),
                    validationErrorMsg.get()
            );
            if (strict) {
              throw new RuntimeException(msg);
            } else {
              LOG.warn(msg);
            }
          }
          // allow each cluster to override the number of workers, if desired
          int numConsumers = (int) clusterConf.getOrDefault("numConsumers", DEFAULT_KAFKA_CONSUMER_COUNT);
          ArrayBlockingQueue<KafkaConsumer> queue = new ArrayBlockingQueue<>(numConsumers);
          for (int i = 0; i < numConsumers; i++) {
            queue.add(createConsumer(clusterConf));
          }
          return new AbstractMap.SimpleEntry<>((String) clusterConf.get("name"), queue);
        })
        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

    this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(workerThreadCount));
  }

  //
  //
  //
  //
  //
  //

  /**
   * Validate an individual cluster's configuration. A cluster's configuration is valid if:
   * a) Looking it up by name in Burrow returns a successful response (not 404)
   * b) Bootstrap servers match those in burrow
   *  i. in strict mode: The bootstrap server list in the config matches the list advertised by Burrow exactly
   *  ii. in normal mode: All entries in the config's bootstrap server list are advertised by Burrow (Burrow could
   * have extras which don't appear in the config).
   *
   * @param clusterConf the cluster configuration
   * @return an optional string which will contain the validation failure message if validation fails or null if
   * validation succeeds
   */
  private Optional<String> validateClusterConf(Map<String, Object> clusterConf) {
    final String clusterName = (String) clusterConf.get("name");
    final Set<String> bootstrapServersFromBurrow;
    try {
      bootstrapServersFromBurrow = new HashSet<>(this.burrow.getClusterBootstrapServers(clusterName));
    } catch (IOException e) {
      this.metrics.burrowClusterDetailReadFailed.labels(clusterName).inc();
      return Optional.of("failed to read cluster detail from Burrow: " + e.getMessage());
    }

    final Map<String, String> clusterConfKafkaSection = (Map<String, String>) clusterConf.get("kafka");
    final Set<String> bootstrapServersFromConfig = Arrays
            .stream(clusterConfKafkaSection.get("bootstrap.servers").split(","))
            .map(String::trim)
            .collect(Collectors.toSet());

    if (this.strict) {
      return bootstrapServersFromBurrow.equals(bootstrapServersFromConfig) ? Optional.empty()
          : Optional.of(String.format(
                      "strict mode on and the list of bootstrap servers in config is not identical to the " +
                              "list advertised by Burrow\nconfig: %s\nburrow: %s",
              String.join(", ", bootstrapServersFromConfig),
              String.join(", ", bootstrapServersFromBurrow)
              ));
    }
    bootstrapServersFromConfig.removeAll(bootstrapServersFromBurrow);
    return bootstrapServersFromConfig.isEmpty() ? Optional.empty()
        : Optional.of(
            "bootstrap.servers contains the following servers which Burrow doesn't advertise: "
                + String.join(", ", bootstrapServersFromConfig));
  }

  private KafkaConsumer createConsumer(Map<String, Object> conf) {
    Properties props = new Properties();
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("group.id", "adhoc.kafka_consumer_freshness");

    // defaults to make this performant out of the box
    props.put("max.poll.records", 1);
    props.put("session.timeout.ms", 6000);
    props.put("send.buffer.bytes", 100);
    props.put("max.partition.fetch.bytes", 100);
    props.put("check.crcs", false);

    props.putAll((Map<String, Object>) conf.get("kafka"));
    return new KafkaConsumer(props);
  }

  void setupForTesting(Burrow burrow, Map<String, ArrayBlockingQueue<KafkaConsumer>> workers,
      ListeningExecutorService executor) {
    this.burrow = burrow;
    this.availableWorkers = workers;
    this.executor = executor;
  }

  @VisibleForTesting
  void run() {
    this.metrics.elapsed.time(() -> {
      List<Burrow.ClusterClient> clusters;
      try {
        clusters = burrow.getClusters();
      } catch (IOException e) {
        LOG.error("Failed to read clusters from burrow", e);
        this.metrics.burrowClustersReadFailed.inc();
        return null;
      }
      clusters.parallelStream()
          // only try to read clusters for which we are configured
          .filter(client -> {
            ArrayBlockingQueue<KafkaConsumer> workers = this.availableWorkers.get(client.getCluster());
            if (workers == null) {
              LOG.info("Skipping cluster '{}' because not configured to connect", client.getCluster());
            }
            return workers != null;
          })
          .peek(clusterClient -> metrics.lastClusterRunAttempt.labels(clusterClient.getCluster()).setToCurrentTime())
          .map(this::measureCluster)
          .forEach(future -> {
            try {
              String cluster = future.get();
              metrics.lastClusterRunSuccessfulAttempt.labels(cluster).setToCurrentTime();
            } catch (Exception e) {
              LOG.error("Failed to measure a cluster", e);
            }
          });
      // have to return something, we are a callable (so we can throw exceptions more easily)
      return null;
    });
  }

  /**
   * Measure the freshness for all consumers in the cluster.
   *
   * @return future containing the cluster name, representing the successful completion of the freshness computation.
   * {@link Future#get()} will throw an exception if the freshness could not be measured for the cluster.
   * @throws RuntimeException if there is a systemic problem that should shutdown the application.
   */
  private ListenableFuture<String> measureCluster(Burrow.ClusterClient client) {
    List<ListenableFuture<List<Object>>> completedConsumers = new ArrayList<>();
    List<String> consumerGroups;
    try {
      consumerGroups = client.consumerGroups();
      // add some sanity to the logs
      Collections.sort(consumerGroups);
    } catch (IOException e) {
      LOG.error("Failed to read groups from burrow for cluster {}", client.getCluster(), e);
      metrics.burrowClustersConsumersReadFailed.labels(client.getCluster()).inc();
      return Futures.immediateFailedFuture(e);
    }

    try {
      ArrayBlockingQueue<KafkaConsumer> workers = this.availableWorkers.get(client.getCluster());
      for (String consumerGroup : consumerGroups) {
        completedConsumers.add(measureConsumer(client, workers, consumerGroup));
      }
    } catch (InterruptedException e) {
      LOG.error("Interrupted while measuring consumers for {}", client.getCluster(), e);
      return Futures.immediateFailedFuture(e);
    } catch (RejectedExecutionException e) {
      LOG.error("Failed to run freshness lookup for cluster {}, shutting down...", client.getCluster(), e);
      throw e;
    }

    // if all the consumer measurements succeed, then we return the cluster name
    // otherwise, Future.get will throw an exception representing the failure to measure a consumer (and thus the
    // failure to successfully monitor the cluster).
    return Futures.whenAllSucceed(completedConsumers).call(client::getCluster, this.executor);
  }

  /**
   * Measure the freshness for all the topic/partitions currently consumed by the given consumer group. To maintain
   * the existing contract, a consumer measurement fails ({@link Future#get()} throws an exception) only if:
   *  - burrow group status lookup fails
   *  - execution is interrupted
   * Failure to actually measure the consumer is swallowed into a log message & metric update; obviously, this is less
   * than ideal for many cases, but it will be addressed later.
   *
   * @param burrow access to burrow
   * @param workers pool of workers available for querying Kafka
   * @param consumerGroup name of the consumer group to measure
   * @return a future representing the measurement of each topic-partition the consumer reads. If any partition
   * failed to be measured the entire future (i.e. calls to {@link Future#get()}) will be considered a failure.
   * @throws InterruptedException if the application is interrupted while waiting for an available worker
   */
  private ListenableFuture<List<Object>> measureConsumer(Burrow.ClusterClient burrow,
                                                         ArrayBlockingQueue<KafkaConsumer> workers,
                                                         String consumerGroup) throws InterruptedException {
    Map<String, Object> status;
    try {
      status = burrow.getConsumerGroupStatus(consumerGroup);
      Preconditions.checkState(status.get("partitions") != null,
          "Burrow response is missing partitions, got {}", status);
    } catch (IOException | IllegalStateException e) {
      // this happens sometimes, when burrow is acting up (e.g. "bad" consumer names)
      LOG.error("Failed to read Burrow status for consumer {}. Skipping", consumerGroup, e);
      metrics.error.labels(burrow.getCluster(), consumerGroup).inc();
      return Futures.immediateFuture(Collections.emptyList());
    }

    boolean anyEndOffsetFound = false;
    List<Map<String, Object>> partitions = (List<Map<String, Object>>) status.get("partitions");
    List<ListenableFuture<?>> partitionFreshnessComputation = new ArrayList<>(partitions.size());
    for (Map<String, Object> state : partitions) {
      String topic = (String) state.get("topic");
      int partition = (int) state.get("partition");
      Map<String, Object> end = (Map<String, Object>) state.get("end");
      if (end == null) {
        LOG.debug("Skipping {}:{} - {}:{} because no offset found",
            burrow.getCluster(), consumerGroup, topic, partition);
        this.metrics.missing.inc();
        continue;
      }
      anyEndOffsetFound = true;
      long offset = Long.parseLong(end.get("offset").toString());
      boolean upToDate = Long.parseLong(state.get("current_lag").toString()) == 0;
      FreshnessTracker.ConsumerOffset consumerState =
          new FreshnessTracker.ConsumerOffset(burrow.getCluster(), consumerGroup, topic, partition, offset,
              upToDate);

      // wait for a consumer to become available
      KafkaConsumer consumer = workers.take();
      ListenableFuture<?> result = this.executor.submit(new FreshnessTracker(consumerState, consumer, metrics));
      // Hand back the consumer to the available workers when the task is complete
      Futures.addCallback(result, new FutureCallback<Object>() {
        @Override
        public void onSuccess(Object o) {
          workers.add(consumer);
        }

        @Override
        public void onFailure(Throwable throwable) {
          metrics.error.labels(burrow.getCluster(), consumerGroup).inc();
          workers.add(consumer);
        }
      }, this.executor);
      partitionFreshnessComputation.add(result);
    }

    // only log if no end-offset found for any partition, it reduces the verbosity by pointing out full consumer
    // groups that are in weird state.
    if (!partitions.isEmpty() && !anyEndOffsetFound) {
      LOG.warn("Skipping {}: {} because no end-offsets found for any of the {} topic/partitions",
          burrow.getCluster(), consumerGroup, partitions.size());
    }

    // these can all fail and NOT mark the cluster as a failure, so we map it into a "success"
    return Futures.whenAllComplete(partitionFreshnessComputation).call(() -> {
      // skip over any future that failed: this consumer is "successful" regardless of each partition's success/failure
      return partitionFreshnessComputation.stream()
          .map(partition -> {
            try {
              return partition.get();
            } catch (Exception e) {
              // skip it!
              return null;
            }
          })
          .filter(Objects::isNull)
          .collect(Collectors.toList());
    }, this.executor);
  }

  private void stop() {
    if (this.executor != null) {
      this.executor.shutdown();

    }
    // stop all the kafka consumers
    if (this.availableWorkers != null) {
      this.availableWorkers.values().stream()
          .flatMap(Collection::stream)
          .forEach(KafkaConsumer::close);
    }
  }

  public FreshnessMetrics getMetricsForTesting() {
    return this.metrics;
  }
}
