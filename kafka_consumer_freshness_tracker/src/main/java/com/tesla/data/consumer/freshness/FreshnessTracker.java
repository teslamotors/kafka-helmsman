/*
 * Copyright © 2018 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tesla.shade.com.google.common.annotations.VisibleForTesting;
import tesla.shade.com.google.common.base.MoreObjects;
import tesla.shade.com.google.common.base.Preconditions;
import tesla.shade.com.google.common.collect.Streams;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

/**
 * Takes a particular instance of a consumer (name, topic, partition, offset) and determines some meta-metrics about
 * that consumer, like its freshness and current queue time.
 */
class FreshnessTracker implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(FreshnessTracker.class);

  private final Consumer kafka;
  private final FreshnessMetrics metrics;
  private final ConsumerOffset consumer;
  private TopicPartition from;
  private Clock clock = Clock.systemUTC();

  FreshnessTracker(ConsumerOffset consumer, Consumer kafka, FreshnessMetrics metrics) {
    this.consumer = consumer;
    this.from = consumer.tp;
    this.kafka = kafka;
    this.metrics = metrics;
  }

  public void run() {
    // assume its zero, aka the Log-End-Offset is also the latest committed offset
    long freshness = 0;
    // its not up-to-date, so there must be an offset after the latest commit in the queue
    if (!this.consumer.upToDate) {
      // ask Kafka for the time the next offset
      long firstUncommittedOffset = consumer.offset + 1;
      Optional<ConsumerRecord> firstUncommittedRecord = this.getRecordAtOffset(firstUncommittedOffset);
      metrics.kafkaRead.labels(this.consumer.cluster, this.consumer.group).inc();
      if (!firstUncommittedRecord.isPresent()) {
        // maybe the processor is very slow (or stream is high volume) and we lost the offset
        LOG.error("Failed to load offset for {} : {}", consumer, firstUncommittedOffset);
        metrics.failed.labels(this.consumer.cluster, this.consumer.group).inc();
        return;
      }

      final ConsumerRecord record = firstUncommittedRecord.get();
      metrics.timestampType.labels(this.consumer.cluster, this.from.topic(), record.timestampType().name).inc();
      if (record.timestamp() < 0) {
        // producer or broker is only supposed to record non-negative timestamps, however
        // a non-standard client or client on a very old version may produce records with
        // negative timestamps -- we ignore those.
        LOG.info("Read invalid offset for {} : {}", consumer, record.timestamp());
        metrics.invalid.labels(this.consumer.cluster, this.consumer.group).inc();
        return;
      }
      Instant now = Instant.now(clock);
      freshness = Math.max(now.toEpochMilli() - record.timestamp(), 0);
      LOG.debug("Found freshness of {} from first uncommitted record {} for {}", freshness, record, consumer);
    } else {
      // up-to-date consumers are at the LEO (i.e. lag == 0), so we don't have a 'first uncommitted record' that
      // would be interesting to log, so just log the freshness == 0. Its in an 'else' to avoid too double logging
      // for consumers that are not up-to-date.
      LOG.debug("{} recording {} ms freshness", consumer, freshness);
    }
    metrics.freshness.labels(this.consumer.cluster, this.consumer.group, this.from.topic(),
        Integer.toString(this.from.partition()))
        .set(freshness);
  }

  private Optional<ConsumerRecord> getRecordAtOffset(long offset) {
    Collection<TopicPartition> tps = Collections.singletonList(from);
    try {
      return metrics.kafkaQueryLatency.labels(this.consumer.cluster, "offset").time(() -> {
        kafka.assign(tps);
        seekFrom(offset);
        ConsumerRecords records = this.kafka.poll(Duration.ofSeconds(2));
        return Streams.<ConsumerRecord>stream(records)
            .findFirst();
      });
    } catch (Throwable t) {
      LOG.error("Failed to read {} from Kafka - maybe an SSL/connectivity error?", consumer, t);
      throw t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
    }
  }

  /**
   * Seeking will point at the latest record, after the current, i.e  the next record in the stream from that offset,
   * which has not been committed. This logically makes sense with the way kafka structures offsets; seeking to the
   * end gives you the offset of the next message to be added to the queue, not the offset of the actual last message.
   * If this is a slow stream, a seek could fail because there is no offset. Thus, we get the specified
   * position, minus one.
   * @param position in the current 'from' position to seek
   */
  private void seekFrom(long position) {
    Preconditions.checkArgument(position >= 0, "Must specify a non-negative position when seeking");
    // seeking to position 1 means seeking to index 0 (beginning) so you can get the first message.
    // seeking to position 0 means going to the beginning
    //  ________________
    //  | 1 | 2 | 3 |...  (positions)
    //  ^   ^   ^   ^
    //  0   1   2   3     (offsets)
    if (position <= 1) {
      this.kafka.seekToBeginning(Collections.singletonList(from));
    } else {
      this.kafka.seek(from, position - 1);
    }
  }

  @VisibleForTesting
  void setClockForTesting(Clock clock) {
    this.clock = clock;
  }

  static class ConsumerOffset {
    final String cluster;
    final String group;
    final TopicPartition tp;
    final long offset;
    final boolean upToDate;

    ConsumerOffset(String cluster, String group, String topic, int partition, long offset, boolean upToDate) {
      this.cluster = cluster;
      this.group = group;
      this.tp = new TopicPartition(topic, partition);
      this.offset = offset;
      this.upToDate = upToDate;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("cluster", cluster)
          .add("group", group)
          .add("tp", tp)
          .add("offset", offset)
          .add("upToDate", upToDate)
          .toString();
    }
  }

  @VisibleForTesting
  ConsumerOffset getConsumerForTesting() {
    return this.consumer;
  }
}
