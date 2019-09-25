/*
 * Copyright © 2018 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import static tesla.shade.com.google.common.collect.Lists.newArrayList;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;
import io.prometheus.client.Summary;

public class FreshnessMetrics {
  Summary elapsed = new Summary.Builder()
      .name("kafka_consumer_freshness_runtime_sec")
      .help("Elapsed runtime (sec) of a single freshness tracker run")
      .create();
  Counter missing = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_missing")
      .help("Number of partitions where there is a consumer, but no offset stored in Burrow")
      .create();
  Counter kafkaRead = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_total_kafka_read")
      .help("Number of offsets loaded from Kafka")
      .labelNames("cluster", "consumer")
      .create();
  Counter failed = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_failed_kafka_read")
      .help("Number of offsets that failed to be loaded from Kafka (consumers too old?)")
      .labelNames("cluster", "consumer")
      .create();
  Counter invalid = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_invalid_kafka_read")
      .help("Number of offsets that loaded from Kafka but were found to be invalid (producer sent invalid timestamp?)")
      .labelNames("cluster", "consumer")
      .create();
  Counter burrowClustersReadFailed = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_failed_burrow_clusters_read")
      .help("Number of times we failed to lookup the clusters from burrow")
      .create();
  Gauge freshness = new Gauge.Builder()
      .name("kafka_consumer_freshness_ms")
      .help("Difference between when the most recent committed offset entered a topic/partition and the timestamp " +
          "of the current Log-End-Offset")
      .labelNames("cluster", "consumer", "topic", "partition")
      .create();
  Histogram kafkaQueryLatency = new Histogram.Builder()
      .name("kafka_consumer_freshness_runtime_kafka_query_time")
      .help("Time spent making the actual queries for kafka time")
      .labelNames("type")
      .create();


  public void register() {
    for (SimpleCollector collector :
        newArrayList(elapsed, missing, kafkaRead, failed, invalid, freshness, kafkaQueryLatency)) {
      collector.register();
    }
  }
}
