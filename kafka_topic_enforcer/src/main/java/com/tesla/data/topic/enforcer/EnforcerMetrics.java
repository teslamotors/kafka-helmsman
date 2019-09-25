/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import io.prometheus.client.Gauge;

public class EnforcerMetrics {

  final Gauge absentTopics = Gauge.build()
      .name("kafka_topic_enforcer_absent_topics")
      .help("Count of topics that are absent from the cluster.")
      .register();

  final Gauge unexpectedTopics = Gauge.build()
      .name("kafka_topic_enforcer_unexpected_topics")
      .help("Count of topics that are unexpectedly present in the cluster.")
      .register();

  final Gauge partitionCountDrift = Gauge.build()
      .name("kafka_topic_enforcer_partition_count_drift_topics")
      .help("Count of topics which have drifted from their desired partition count.")
      .labelNames("type")
      .register();

  final Gauge topicConfigDrift = Gauge.build()
      .name("kafka_topic_enforcer_topic_config_drift_topics")
      .help("Count of topics which have drifted from their desired topic config.")
      .labelNames("type")
      .register();

  final Gauge configuredTopics = Gauge.build()
      .name("kafka_topic_enforcer_configured_topics")
      .help("Count of configured topics.")
      .register();

}
