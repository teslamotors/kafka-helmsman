/*
 * Copyright © 2019 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static tesla.shade.com.google.common.collect.Lists.newArrayList;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import tesla.shade.com.google.common.collect.Lists;
import tesla.shade.com.google.common.util.concurrent.ListenableFuture;
import tesla.shade.com.google.common.util.concurrent.ListeningExecutorService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class ConsumerFreshnessTest {

  @Test
  public void testTranslateZeroLagAsUpToDate() throws Exception {
    Burrow burrow = mock(Burrow.class);
    Burrow.ClusterClient client = mockClusterState("cluster1", "group1",
        partitionState("topic1", 1, 10, 0));
    when(burrow.getClusters()).thenReturn(newArrayList(client));

    ListeningExecutorService executor = mock(ListeningExecutorService.class);
    ListenableFuture future = mock(ListenableFuture.class);
    when(executor.submit(any((FreshnessTracker.class)))).thenReturn(future);

    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers("cluster1"), executor);
    freshness.run();

    ArgumentCaptor<FreshnessTracker> argument = ArgumentCaptor.forClass(FreshnessTracker.class);
    verify(executor).submit(argument.capture());
    FreshnessTracker.ConsumerOffset offset = argument.getValue().getConsumerForTesting();
    assertEquals("cluster1", offset.cluster);
    assertEquals("group1", offset.group);
    assertEquals("topic1", offset.tp.topic());
    assertEquals(1, offset.tp.partition());
    assertEquals(10, offset.offset);
    assertTrue("Consumer should not be lagging", offset.upToDate);

    // we should be waiting until the future is complete before returning
    verify(future).get();
  }

  /**
   * Regression test for parsing the current_lag as an integer.
   */
  @Test
  public void testLargeCurrentLag() throws Exception {
    Burrow burrow = mock(Burrow.class);
    Burrow.ClusterClient client = mockClusterState("cluster1", "group1",
        partitionState("topic1", 1, 10, 2285339353L));
    when(burrow.getClusters()).thenReturn(newArrayList(client));

    ListeningExecutorService executor = mock(ListeningExecutorService.class);
    ListenableFuture future = mock(ListenableFuture.class);
    when(executor.submit(any((FreshnessTracker.class)))).thenReturn(future);

    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers("cluster1"), executor);
    freshness.run();

    ArgumentCaptor<FreshnessTracker> argument = ArgumentCaptor.forClass(FreshnessTracker.class);
    verify(executor).submit(argument.capture());
    FreshnessTracker.ConsumerOffset offset = argument.getValue().getConsumerForTesting();
    assertEquals("cluster1", offset.cluster);
    assertEquals("group1", offset.group);
    assertEquals("topic1", offset.tp.topic());
    assertEquals(1, offset.tp.partition());
    assertEquals(10, offset.offset);
    assertFalse("Consumer should be lagging", offset.upToDate);

    // we should be waiting until the future is complete before returning
    verify(future).get();
  }

  /**
   * If burrow is having problems, we should fail because there is no more useful work to do
   */
  @Test(expected = RuntimeException.class)
  public void testFailingToReadBurrowExitsTracker() throws Exception {
    Burrow burrow = mock(Burrow.class);
    when(burrow.getClusters()).thenThrow(new RuntimeException("injected"));
    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers(), null);

    freshness.run();
  }

  /**
   * If burrow is having problems, we should fail because there is no more useful work to do
   */
  @Test(expected = RuntimeException.class)
  public void testFailingToReadConsumerGroupExitsTracker() throws Exception {
    Burrow burrow = mock(Burrow.class);
    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers("cluster"), null);

    Burrow.ClusterClient client = mockClusterState("cluster", "group");
    when(burrow.getClusters()).thenReturn(newArrayList(client));
    when(client.consumerGroups()).thenThrow(new RuntimeException("injected"));
    freshness.run();
  }

  /**
   * Something is quite wrong with the executor, so give up. We assume the executor will just queue outstanding work,
   * so this means something is very wrong.
   */
  @Test(expected = RuntimeException.class)
  public void testFailToSubmitTaskExitsTracker() throws Exception {
    Burrow burrow = mock(Burrow.class);
    ListeningExecutorService executor = mock(ListeningExecutorService.class);
    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers("cluster"), executor);

    Burrow.ClusterClient client = mockClusterState("cluster", "group",
        partitionState("t", 1, 1, 0));
    when(burrow.getClusters()).thenReturn(newArrayList(client));
    when(client.consumerGroups()).thenThrow(new RuntimeException("injected"));

    when(executor.submit(any(FreshnessTracker.class))).thenThrow(new RuntimeException("injected"));
    freshness.run();
  }

  private Map<String, ArrayBlockingQueue<KafkaConsumer>> workers(String... clusters) {
    Map<String, ArrayBlockingQueue<KafkaConsumer>> workers = new HashMap<>(1);
    for (String cluster : clusters) {
      KafkaConsumer consumer = mock(KafkaConsumer.class);
      ArrayBlockingQueue<KafkaConsumer> queue = new ArrayBlockingQueue<>(1);
      queue.add(consumer);
      workers.put(cluster, queue);
    }
    return workers;
  }

  /**
   * Mock a state of the cluster that has a single consumer group for a number of partitions
   *
   * @param cluster name of the cluster
   * @param group name of the consumer group
   * @param partitions partitions the consumer is "reading"
   * @return mock client
   * @see #partitionState(String, int, long, long) helper function to more easily generate partition description
   */
  private Burrow.ClusterClient mockClusterState(String cluster, String group,
      Map<String, Object>... partitions) throws IOException {
    Burrow.ClusterClient client = mock(Burrow.ClusterClient.class);

    when(client.getCluster()).thenReturn(cluster);
    when(client.consumerGroups()).thenReturn(newArrayList(group));

    Map<String, Object> status = new HashMap<>();
    status.put("partitions", Lists.newArrayList(partitions));
    when(client.getConsumerGroupStatus(group)).thenReturn(status);

    return client;
  }

  /**
   * Generate the state as returned by the {@link Burrow} client representing the consumer of a topic/partition.
   *
   * @param topic topic name
   * @param partitionNumber index of the partition
   * @param endOffset most recent offset "read" by the consumer
   * @param currentLagMs amount of lag, in milliseconds, the consumer is behind the Log-Eng-Offset
   */
  private Map<String, Object> partitionState(String topic, int partitionNumber, long endOffset, long currentLagMs) {
    Map<String, Object> partition = new HashMap<>();
    partition.put("topic", topic);
    partition.put("partition", partitionNumber);
    partition.put("current_lag", currentLagMs);
    Map<String, Object> end = new HashMap<>();
    end.put("offset", endOffset);
    partition.put("end", end);
    return partition;
  }
}
