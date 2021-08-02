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

  final Summary elapsed;
  final Counter missing;
  final Counter kafkaRead;
  final Counter failed;
  final Counter invalid;
  final Counter error;
  final Counter burrowClustersConsumersReadFailed;
  final Counter burrowClustersReadFailed;
  final Gauge freshness;
  final Histogram kafkaQueryLatency;
  final Gauge lastClusterRunAttempt;
  final Gauge lastClusterRunSuccessfulAttempt;

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
    elapsed = new Summary.Builder()
        .name("kafka_consumer_freshness_runtime_sec")
        .help("Elapsed runtime (sec) of a single freshness tracker run")
        .register();
    missing = new Counter.Builder()
        .name("kafka_consumer_freshness_runtime_missing")
        .help("Number of partitions where there is a consumer, but no offset stored in Burrow")
        .register();
    kafkaRead = new Counter.Builder()
        .name("kafka_consumer_freshness_runtime_total_kafka_read")
        .help("Number of offsets loaded from Kafka")
        .labelNames("cluster", "consumer")
        .register();
    failed = new Counter.Builder()
        .name("kafka_consumer_freshness_runtime_failed_kafka_read")
        .help("Number of offsets that failed to be loaded from Kafka (consumers too old?)")
        .labelNames("cluster", "consumer")
        .register();
    invalid = new Counter.Builder()
        .name("kafka_consumer_freshness_runtime_invalid_kafka_read")
        .help("Number of offsets that loaded from Kafka but were found to be invalid (producer sent invalid timestamp?)")
        .labelNames("cluster", "consumer")
        .register();
    error = new Counter.Builder()
        .name("kafka_consumer_freshness_runtime_query_exception")
        .help("Number of Kafka queries that threw an exception")
        .labelNames("cluster", "consumer")
        .register();
    burrowClustersConsumersReadFailed = new Counter.Builder()
        .name("kafka_consumer_freshness_runtime_failed_burrow_cluster_consumers_read")
        .help("Number of times we failed to lookup a cluster's consumers from burrow")
        .labelNames("cluster")
        .register();
    burrowClustersReadFailed = new Counter.Builder()
        .name("kafka_consumer_freshness_runtime_failed_burrow_clusters_read")
        .help("Number of times we failed to lookup the clusters from burrow")
        .register();
    freshness = new Gauge.Builder()
        .name("kafka_consumer_freshness_ms")
        .help("Difference between when the most recent committed offset entered a topic/partition and the timestamp " +
            "of the current Log-End-Offset")
        .labelNames("cluster", "consumer", "topic", "partition")
        .register();
    kafkaQueryLatency = new Histogram.Builder()
        .name("kafka_consumer_freshness_runtime_kafka_query_time")
        .help("Time spent making the actual queries for kafka time")
        .labelNames("cluster", "type")
        .register();
    lastClusterRunAttempt = Gauge.build()
        .name("kafka_consumer_freshness_last_run_timestamp")
        .help("Timestamp of last run as epoch seconds.")
        .labelNames("cluster")
        .register();
    lastClusterRunSuccessfulAttempt = Gauge.build()
        .name("kafka_consumer_freshness_last_success_run_timestamp")
        .help("Timestamp of last successful run as epoch seconds.")
        .labelNames("cluster")
        .register();
  }

}
