/*
 * Copyright © 2018 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;

public class FreshnessMetrics {

  public FreshnessMetrics() {
    /*
    The ideal way to register a metric is to define a static class variable, but
    that approach does not allow us to unit-test the instrumentation code path, since
    within tests, we have to 'reset' metric values. Prometheus registries themselves are
    static and  do not allow re-registration of a metric. Finally, we want 'in-line'
    registration of metrics as opposed to explicit registration. Given these constraints,  we
    clear the default prometheus registry which removes all previous registrations.
     */
    CollectorRegistry.defaultRegistry.clear();
  }

  Summary elapsed = new Summary.Builder()
      .name("kafka_consumer_freshness_runtime_sec")
      .help("Elapsed runtime (sec) of a single freshness tracker run")
      .register();
  Counter missing = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_missing")
      .help("Number of partitions where there is a consumer, but no offset stored in Burrow")
      .register();
  Counter kafkaRead = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_total_kafka_read")
      .help("Number of offsets loaded from Kafka")
      .labelNames("cluster", "consumer")
      .register();
  Counter failed = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_failed_kafka_read")
      .help("Number of offsets that failed to be loaded from Kafka (consumers too old?)")
      .labelNames("cluster", "consumer")
      .register();
  Counter invalid = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_invalid_kafka_read")
      .help("Number of offsets that loaded from Kafka but were found to be invalid (producer sent invalid timestamp?)")
      .labelNames("cluster", "consumer")
      .register();
  Counter error = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_query_exception")
      .help("Number of Kafka queries that threw an exception")
      .labelNames("cluster", "consumer")
      .register();
  Counter burrowClustersReadFailed = new Counter.Builder()
      .name("kafka_consumer_freshness_runtime_failed_burrow_clusters_read")
      .help("Number of times we failed to lookup the clusters from burrow")
      .register();
  Gauge freshness = new Gauge.Builder()
      .name("kafka_consumer_freshness_ms")
      .help("Difference between when the most recent committed offset entered a topic/partition and the timestamp " +
          "of the current Log-End-Offset")
      .labelNames("cluster", "consumer", "topic", "partition")
      .register();
  Histogram kafkaQueryLatency = new Histogram.Builder()
      .name("kafka_consumer_freshness_runtime_kafka_query_time")
      .help("Time spent making the actual queries for kafka time")
      .labelNames("type")
      .register();
}
