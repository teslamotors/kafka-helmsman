/*
 * Copyright © 2018 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Test;
import org.mockito.InOrder;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FreshnessTrackerTest {

  @Test
  public void testNoLag() throws Exception {
    FreshnessTracker.ConsumerOffset consumer =
        new FreshnessTracker.ConsumerOffset("cluster", "group", "topic", 1, 1, true);
    FreshnessMetrics metrics = new FreshnessMetrics();
    Consumer kafka = mock(Consumer.class);

    FreshnessTracker work = new FreshnessTracker(consumer, kafka, metrics);
    work.run();

    assertEquals("Freshness should be 0 if no lag", 0,
        metrics.freshness.labels("cluster", "group", "topic", "1").get(), 0);
    assertEquals(0, metrics.failed.labels("cluster", "group").get(), 0);
    verifyNoInteractions(kafka);
  }

  @Test
  public void testInvalidOffset() {
    FreshnessTracker.ConsumerOffset consumer =
        new FreshnessTracker.ConsumerOffset("cluster", "group", "topic", 1, 1, false);
    ConsumerRecords records = createConsumerRecordsAtTimestamps(-1L);
    Consumer kafka = mock(Consumer.class);
    when(kafka.poll(any(Duration.class))).thenReturn(records);
    FreshnessMetrics metrics = new FreshnessMetrics();
    new FreshnessTracker(consumer, kafka, metrics).run();
    assertEquals(1, metrics.invalid.labels("cluster", "group").get(), 0);
    assertEquals(0,
        metrics.freshness.labels("cluster", "group", "topic", "1").get(), 0);
  }

  /**
   * We now have two messages, one the most recent commit, one the Log-End-Offset
   * | t0 | t1 | ...
   * ^ - LEO
   * ^ - committed
   * So the lag is the residency time of t1 in the queue, or (now) - (t1)
   */
  @Test
  public void testUncommittedForOneMillisecond() throws Exception {
    // start at 'now'
    Clock current = Clock.fixed(Instant.now(), Clock.systemDefaultZone().getZone());
    // (t1) is now - 1
    long ingestTimestamp = current.millis() - 1;
    ConsumerRecords record = createConsumerRecordsAtTimestamps(ingestTimestamp);

    Consumer kafka = mock(Consumer.class);
    when(kafka.poll(Duration.ofSeconds(2))).thenReturn(record);

    // (t0) is now - 2
    FreshnessTracker.ConsumerOffset consumer =
        new FreshnessTracker.ConsumerOffset("cluster", "group", "topic", 1, 1, false);
    FreshnessMetrics metrics = new FreshnessMetrics();

    FreshnessTracker work = new FreshnessTracker(consumer, kafka, metrics);
    work.setClockForTesting(current);
    work.run();

    assertEquals("Should be behind the LEO timestamp", 1,
        metrics.freshness.labels("cluster", "group", "topic", "1").get(), 0);
    assertEquals(1, metrics.timestampType.labels("cluster", "topic", "LogAppendTime").get(), 0);
    assertEquals(0, metrics.failed.labels("cluster", "group").get(), 0);
    TopicPartition tp = new TopicPartition("topic", 1);
    Collection<TopicPartition> tps = Collections.singletonList(tp);
    verify(kafka).assign(tps);
    verify(kafka).seek(tp, 1);
    verify(kafka).poll(Duration.ofSeconds(2));
    verifyNoMoreInteractions(kafka);
  }


  @Test
  public void testCouldNotLoadLatestTimestamp() throws Exception {
    // aka, doesn't return any records at the next offset
    ConsumerRecords logEndOffsetTimestamp = createConsumerRecordsAtTimestamps();
    Consumer kafka = mock(Consumer.class);
    when(kafka.poll(Duration.ofSeconds(2))).thenReturn(logEndOffsetTimestamp);

    FreshnessTracker.ConsumerOffset consumer =
        new FreshnessTracker.ConsumerOffset("cluster", "group", "topic", 1, 1, false);
    FreshnessMetrics metrics = new FreshnessMetrics();
    FreshnessTracker work = new FreshnessTracker(consumer, kafka, metrics);
    work.run();

    assertEquals("Freshness should not be set", 0, metrics.freshness.labels("cluster", "group", "topic", "1").get(), 0);
    verify(kafka).poll(Duration.ofSeconds(2));
    TopicPartition tp = new TopicPartition("topic", 1);
    Collection<TopicPartition> tps = Collections.singletonList(tp);
    verify(kafka).assign(tps);
    verify(kafka).seek(tp, 1);
    verifyNoMoreInteractions(kafka);
  }

  /**
   * This is a somewhat contrived test case . The first committed offset should be the offset at (0), but this is
   * checking if that isn't the latest (consumer is not up-to-date) but somehow even got something committed. Its
   * strange, but has been seen to happen with old consumers whose offset have fallen off the stream or other,
   * strange cases given the asynchronous nature of the interactions between burrow and kafka.
   */
  @Test
  public void testSeekToBeginningForFirstOffset() throws Exception {
    Clock clock = Clock.fixed(Instant.now(), Clock.systemDefaultZone().getZone());
    long ts = clock.millis() - 1;
    ConsumerRecords recordsOneMilliBefore = createConsumerRecordsAtTimestamps(ts);
    Consumer kafka = mock(Consumer.class);
    when(kafka.poll(Duration.ofSeconds(2))).thenReturn(recordsOneMilliBefore);

    // this consumer is at the first position (offset 0), so we expect to go back one offset
    FreshnessTracker.ConsumerOffset consumer =
        new FreshnessTracker.ConsumerOffset("cluster", "group", "topic", 1, 0, false);
    FreshnessMetrics metrics = new FreshnessMetrics();
    FreshnessTracker work = new FreshnessTracker(consumer, kafka, metrics);
    work.setClockForTesting(clock);
    work.run();

    InOrder ordered = inOrder(kafka);
    // seeking the committed offset, but go back 1 to actual message
    ordered.verify(kafka).seekToBeginning(Collections.singletonList(new TopicPartition("topic", 1)));
    ordered.verify(kafka).poll(Duration.ofSeconds(2));
    ordered.verifyNoMoreInteractions();

    double noDelta = 0;
    assertEquals("Freshness should be zero for no lag",
        1, metrics.freshness.labels("cluster", "group", "topic", "1").get(), noDelta);
    assertEquals(0, metrics.failed.labels("cluster", "group").get(), 0);
  }

  private ConsumerRecords createConsumerRecordsAtTimestamps(long... timestamps) {
    List<ConsumerRecord> consumerRecords = new ArrayList<>();
    // setup the records to return
    for (long ts : timestamps) {
      ConsumerRecord record = mock(ConsumerRecord.class);
      when(record.timestamp()).thenReturn(ts);
      when(record.timestampType()).thenReturn(TimestampType.LOG_APPEND_TIME);
      consumerRecords.add(record);
    }
    Map<TopicPartition, List<ConsumerRecord>> map = new HashMap<>();
    map.put(null, consumerRecords);
    return new ConsumerRecords(map);
  }
}
