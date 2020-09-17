/*
 * Copyright © 2018 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import static java.lang.System.exit;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tesla.shade.com.google.common.annotations.VisibleForTesting;
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
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
        LOG.info(client.newCall(request).execute().body().string());
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
    this.burrow = new Burrow((Map<String, Object>) conf.get("burrow"));
    int workerThreadCount = (int) conf.getOrDefault("workerThreadCount", DEFAULT_WORKER_THREADS);
    this.availableWorkers = ((List<Map<String, Object>>) conf.get("clusters")).stream()
        .map(clusterConf -> {
          // allow each cluster to override the number of workers, if desired
          int numConsumers = (int) clusterConf.getOrDefault("numConsumers", DEFAULT_KAFKA_CONSUMER_COUNT);
          ArrayBlockingQueue<KafkaConsumer> queue = new ArrayBlockingQueue<>(numConsumers);
          for (int i = 0; i < numConsumers; i++) {
            queue.add(createConsumer(clusterConf));
          }
          return new AbstractMap.SimpleEntry<>((String) clusterConf.get("name"), queue);
        }).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));

    this.executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(workerThreadCount));
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
        LOG.error("Failed to read from burrow", e);
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
          // do the work of reading consumers per-cluster
          .map(this::measureCluster)
          // wait for all the futures to complete
          .flatMap(Collection::parallelStream)
          .forEach(future -> {
            try {
              future.get();
            } catch (Exception e) {
              LOG.error("Failed to resolve a consumer", e);
            }
          });
      // have to return something, we are a callable (so we can throw exceptions more easily)
      return null;
    });
  }

  private List<ListenableFuture> measureCluster(Burrow.ClusterClient client) {
    List<ListenableFuture> completed = new ArrayList<>();
    try {
      metrics.lastClusterRunAttempt.labels(client.getCluster()).setToCurrentTime();
      ArrayBlockingQueue<KafkaConsumer> workers = this.availableWorkers.get(client.getCluster());
      List<String> consumerGroups = client.consumerGroups();
      Collections.sort(consumerGroups); // add some sanity to the logs
      for (String consumerGroup : consumerGroups) {
        Map<String, Object> status;
        try {
          status = client.getConsumerGroupStatus(consumerGroup);
        } catch (JsonParseException e) {
          // this happens sometimes, when burrow is acting up
          LOG.error("Failed to read Burrow status for consumer {}. Skipping", consumerGroup, e);
          continue;
        }
        for (Map<String, Object> state : (List<Map<String, Object>>) status.get("partitions")) {
          String topic = (String) state.get("topic");
          int partition = (int) state.get("partition");
          Map<String, Object> end = (Map<String, Object>) state.get("end");
          if (end == null) {
            LOG.error("Skipping {} - {}:{} because no offset found", consumerGroup, topic, partition);
            this.metrics.missing.inc();
            continue;
          }
          long offset = Long.valueOf(end.get("offset").toString());
          boolean upToDate = Long.valueOf(state.get("current_lag").toString()) == 0;
          FreshnessTracker.ConsumerOffset consumerState =
              new FreshnessTracker.ConsumerOffset(client.getCluster(), consumerGroup, topic, partition, offset,
                  upToDate);

          // wait for a worker to become available
          KafkaConsumer worker = workers.take();
          ListenableFuture result = this.executor.submit(
              new FreshnessTracker(consumerState, worker, metrics));
          // Hand back the consumer to the available workers when the task is complete
          Futures.addCallback(result, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object o) {
              workers.add(worker);
            }

            @Override
            public void onFailure(Throwable throwable) {
              metrics.error.labels(client.getCluster(), consumerGroup);
              workers.add(worker);
            }
          }, this.executor);
          completed.add(result);
        }
      }
      metrics.lastClusterRunSuccessfulAttempt.labels(client.getCluster()).setToCurrentTime();
    } catch (IOException | InterruptedException e) {
      LOG.error("Failed to handle cluster {}", client.getCluster(), e);
    }
    return completed;
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
}
