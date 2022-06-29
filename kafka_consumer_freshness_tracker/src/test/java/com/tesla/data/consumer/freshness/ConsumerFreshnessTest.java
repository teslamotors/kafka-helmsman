/*
 * Copyright © 2019 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_CHECKSUM;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static tesla.shade.com.google.common.collect.Lists.newArrayList;

import com.google.common.collect.ImmutableMap;
import io.prometheus.client.Gauge;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.record.TimestampType;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import tesla.shade.com.google.common.collect.Lists;
import tesla.shade.com.google.common.util.concurrent.ListeningExecutorService;
import tesla.shade.com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Validate that we compute freshness and handle errors as expected. Specifically, for errors, we do the following:
 *  * rejected freshness lookup -> fail the application (something very wrong)
 *  * interrupted while waiting for a consumer -> mark the cluster as failed
 *  * freshness computation throws an exception -> marks the group as a failure
 *  * Burrow errors (failed response, bad data, etc.)
 *    * find all clusters -> fail run, mark error
 *    * find all consumers in a cluster -> mark the cluster as failed
 *    * find status of a consumer -> skip the consumer, mark the consumer as failed, but not the cluster
 *    * missing partition end offset -> mark missing, skip partition
 */
public class ConsumerFreshnessTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testTranslateZeroLagAsUpToDate() throws Exception {
    Burrow burrow = mock(Burrow.class);
    Burrow.ClusterClient client = mockClusterState("cluster1", "group1",
        partitionState("topic1", 1, 10, 0));
    when(burrow.getClusters()).thenReturn(newArrayList(client));

    withExecutor(executor -> {
      Map<String, ArrayBlockingQueue<KafkaConsumer>> workers = workers("cluster1");
      ConsumerFreshness freshness = new ConsumerFreshness();
      freshness.setupForTesting(burrow, workers, executor);
      freshness.run();

      FreshnessMetrics metrics = freshness.getMetricsForTesting();
      assertEquals("Should be no log for an up-to-date consumer", 0,
          metrics.freshness.labels("cluster1", "group1", "topic1", "1").get(),
          0.0);

      // if there is no burrow lag, we shouldn't even try to read kafka
      try {
        verifyZeroInteractions(workers.get("cluster1").take());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Regression test for parsing the current_lag as an integer.
   */
  @Test
  public void testLargeCurrentLag() throws Exception {
    long lagMs = 2285339353L;
    Burrow burrow = mock(Burrow.class);
    Burrow.ClusterClient client = mockClusterState("cluster1", "group1",
        partitionState("topic1", 1, 10, lagMs));
    when(burrow.getClusters()).thenReturn(newArrayList(client));

    withExecutor(executor ->{
      KafkaConsumer consumer = mock(KafkaConsumer.class);
      when(consumer.poll(Mockito.any(Duration.class))).thenReturn(records("topic", 1, 10, lagMs));
      ConsumerFreshness freshness = new ConsumerFreshness();
      freshness.setupForTesting(burrow, workers("cluster1", consumer), executor);
      freshness.run();

      FreshnessMetrics metrics = freshness.getMetricsForTesting();
      assertSuccessfulClusterMeasurement(freshness, "cluster1");
      Gauge.Child measurement = metrics.freshness.labels("cluster1", "group1", "topic1", "1");
      assertTrue("Should have at least the specified lag for the group " + lagMs + ", but found" + measurement.get(),
          measurement.get() >= lagMs);
    });
  }

  @Test
  public void testFailClusterWhenInterruptedWaitingForAConsumer() throws Exception {
    Burrow burrow = mock(Burrow.class);
    Burrow.ClusterClient client = mockClusterState("cluster1", "group1",
        partitionState("topic1", 1, 10, 10L));
    when(burrow.getClusters()).thenReturn(newArrayList(client));

    withExecutor(executor ->{
      // create an empty queue of workers so #take blocks forever, giving us the opportunity to interrupt the thread.
      Map<String, ArrayBlockingQueue<KafkaConsumer>> workers = new HashMap<>(1);
      ArrayBlockingQueue<KafkaConsumer> queue = new ArrayBlockingQueue<>(1);
      workers.put("cluster1", queue);

      ConsumerFreshness freshness = new ConsumerFreshness();
      freshness.setupForTesting(burrow, workers, executor);

      // run it in a separate thread to emulate it running as a process that gets interrupted
      Thread t = new Thread(freshness::run);
      t.start();
      t.interrupt();
      try {
        t.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      assertNoSuccessfulClusterMeasurement(freshness, "cluster1");
    });
  }

  @Test
  public void testFailConsumerButNotClusterIfComputationFails() throws Exception {
    Burrow burrow = mock(Burrow.class);
    Burrow.ClusterClient client = mockClusterState("cluster1", "group1",
        partitionState("topic1", 0, 10, 10L),
        partitionState("topic1", 1, 10, 10L)
    );
    when(burrow.getClusters()).thenReturn(newArrayList(client));

    withExecutor(executor -> {
      KafkaConsumer consumer = mock(KafkaConsumer.class);
      // first consumer lookup fails, the second one success
      when(consumer.poll(Mockito.any(Duration.class)))
          .thenThrow(new RuntimeException("injected"))
          .thenReturn(records("topic", 1, 10, 10L));

      ConsumerFreshness freshness = new ConsumerFreshness();
      freshness.setupForTesting(burrow, workers("cluster1", consumer), executor);
      freshness.run();

      assertSuccessfulClusterMeasurement(freshness, "cluster1");
      FreshnessMetrics metrics = freshness.getMetricsForTesting();
      assertEquals(1, metrics.error.labels("cluster1", "group1").get(), 1.0);
    });
  }

  /**
   * If burrow is having problems, we should fail because there is no more useful work to do
   */
  @Test
  public void testBurrowFailReadClustersRuntime() throws Exception {
    Burrow burrow = mock(Burrow.class);
    when(burrow.getClusters()).thenThrow(new RuntimeException("injected"));
    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers(), null);
    thrown.expect(RuntimeException.class);
    freshness.run();
  }

  @Test
  public void testBurrowFailReadClustersIOException() throws Exception {
    Burrow burrow = mock(Burrow.class);
    when(burrow.getClusters()).thenThrow(new IOException("injected"));
    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers(), null);
    freshness.run();
    assertEquals("IOException from burrow should mark the burrowClustersReadFailed metric", 1,
        freshness.getMetricsForTesting().burrowClustersReadFailed.get(), 0.0);
  }

  /**
   * If burrow is having problems, we should ride over consumer group lookup failures.
   */
  @Test
  public void testBurrowFailingToReadConsumerGroupsMarksClusterFailure() throws Exception {
    Burrow burrow = mock(Burrow.class);
    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers("cluster"), null);

    Burrow.ClusterClient client = mockClusterState("cluster", "group");
    when(burrow.getClusters()).thenReturn(newArrayList(client));
    when(client.consumerGroups()).thenThrow(new IOException("injected"));
    freshness.run();
    assertEquals(1.0, freshness.getMetricsForTesting().burrowClustersConsumersReadFailed.labels("cluster").get(), 0.0);
    assertNoSuccessfulClusterMeasurement(freshness, "cluster");
  }

  @Test
  public void testBurrowFailingToReadConsumerGroupStatusMarksGroupError() throws Exception {
    Burrow burrow = mock(Burrow.class);
    ConsumerFreshness freshness = new ConsumerFreshness();
    withExecutor(executor -> {
      freshness.setupForTesting(burrow, workers("cluster"), executor);

      try {
        Burrow.ClusterClient client = mockClusterState("cluster", "group");
        when(burrow.getClusters()).thenReturn(newArrayList(client));
        when(client.consumerGroups()).thenReturn(newArrayList("group"));
        when(client.getConsumerGroupStatus("group")).thenThrow(mock(IOException.class));
        freshness.run();
        assertEquals(1.0, freshness.getMetricsForTesting().error.labels("cluster", "group").get(), 0.0);
        // failing all the groups status lookup should not fail the cluster. Feels weird, but it's the current behavior
        assertNoSuccessfulClusterMeasurement(freshness, "cluster");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testBurrowMissingConsumerGroupPartitionsMarksErrorForGroup() throws Exception {
    Burrow burrow = mock(Burrow.class);
    ConsumerFreshness freshness = new ConsumerFreshness();
    withExecutor(executor -> {
      freshness.setupForTesting(burrow, workers("cluster"), executor);

      try {
        Burrow.ClusterClient client = mockClusterState("cluster", "group");
        when(burrow.getClusters()).thenReturn(newArrayList(client));
        when(client.consumerGroups()).thenReturn(newArrayList("group"));
        when(client.getConsumerGroupStatus("group")).thenReturn(new HashMap<>());
        freshness.run();
        assertEquals(1.0, freshness.getMetricsForTesting().error.labels("cluster", "group").get(), 0.0);
        // no consumer group was successful, cluster is not successful
        assertNoSuccessfulClusterMeasurement(freshness, "cluster");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testBurrowMissingConsumerGroupPartitionEndOffsetMarksMissing() throws Exception {
    Burrow burrow = mock(Burrow.class);
    ConsumerFreshness freshness = new ConsumerFreshness();
    withExecutor(executor -> {
      freshness.setupForTesting(burrow, workers("cluster"), executor);

      try {
        Burrow.ClusterClient client = mockClusterState("cluster", "group");
        when(burrow.getClusters()).thenReturn(newArrayList(client));
        when(client.consumerGroups()).thenReturn(newArrayList("group"));
        when(client.getConsumerGroupStatus("group")).thenReturn(ImmutableMap.of(
            "partitions", newArrayList(ImmutableMap.of(
                "topic", "some-topic",
                "partition", 1
                )
            )));
        freshness.run();
        assertEquals(1.0, freshness.getMetricsForTesting().missing.get(), 0.0);
        // no consumer group was successful, cluster is not successful
        assertNoSuccessfulClusterMeasurement(freshness, "cluster");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Test
  public void testConfigurationContainsUnknownCluster() throws Exception {
    Burrow burrow = mock(Burrow.class);
    String clusterName = "bad_cluster";
    when(burrow.getClusterBootstrapServers(clusterName)).thenThrow(new IOException());
    Map<String, Object> conf = mockConfForCluster(
            clusterName, "l1.example.com:9092", "l2.example.com:9092"
    );

    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.burrow = burrow;

    Optional<String> msg = freshness.validateClusterConf(conf);
    Assert.assertTrue(msg.isPresent());
    Assert.assertTrue(msg.get().contains("failed to read cluster detail from Burrow: "));
    Assert.assertEquals(1.0,
            freshness.getMetricsForTesting().burrowClusterDetailReadFailed.labels(clusterName).get(), 0.0);
  }

  @Test
  public void testConfigurationIsValidServersMatchBurrow() throws Exception {
    Burrow burrow = mock(Burrow.class);
    String clusterName = "cluster1";
    when(burrow.getClusterBootstrapServers(clusterName))
            .thenReturn(Arrays.asList("kafka01.example.com:10251", "kafka02.example.com:10251")
      );

    Map<String, Object> conf = mockConfForCluster(
        clusterName,
        "kafka01.example.com:10251", "kafka02.example.com:10251"
        );

    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.burrow = burrow;

    // normal mode
    Assert.assertFalse(freshness.validateClusterConf(conf).isPresent());

    // strict mode
    freshness.strict = true;
    Assert.assertFalse(freshness.validateClusterConf(conf).isPresent());
  }

  @Test
  public void testConfigurationIsInvalidContainsUnknownServer() throws Exception {
    Burrow burrow = mock(Burrow.class);
    String clusterName = "cluster1";
    List<String> burrowBootstrapServers = Arrays.asList(
            "kafka01.example.com:10251", "kafka02.example.com:10251");
    when(burrow.getClusterBootstrapServers(clusterName))
      .thenReturn(burrowBootstrapServers);

    List<String> confBootstrapServers = Arrays.asList("kafka01.example.com:10251", "kafka02.example.com:10251",
            "kafka03.example.com:10251");

    Map<String, Object> conf = mockConfForCluster(
        clusterName,
        confBootstrapServers.toArray(new String[0])
        );

    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.burrow = burrow;

    String expected = String.format(
            "the set of bootstrap servers in config is not the same as the " +
                    "set advertised by Burrow\nconfig: %s\nburrow: %s",
            String.join(", ", confBootstrapServers),
            String.join(", ", burrowBootstrapServers)
    );

    // normal mode
    Assert.assertEquals(Optional.of(expected), freshness.validateClusterConf(conf));

    // strict mode
    freshness.strict = true;
    Assert.assertEquals(Optional.of(expected), freshness.validateClusterConf(conf));
  }

  @Test
  public void testConfigurationIsInvalidMissingServer() throws Exception {
    Burrow burrow = mock(Burrow.class);
    String clusterName = "cluster1";
    List<String> burrowBootstrapServers = Arrays.asList("kafka01.example.com:10251", "kafka02.example.com:10251",
            "kafka03.example.com:10251");
    List<String> confBootstrapServers = Arrays.asList("kafka01.example.com:10251", "kafka02.example.com:10251");

    when(burrow.getClusterBootstrapServers(clusterName))
      .thenReturn(burrowBootstrapServers);

    Map<String, Object> conf = mockConfForCluster(
        clusterName,
            confBootstrapServers.toArray(new String[0])
        );

    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.burrow = burrow;

    String expected = String.format(
            "the set of bootstrap servers in config is not the same as the " +
                    "set advertised by Burrow\nconfig: %s\nburrow: %s",
            String.join(", ", confBootstrapServers),
            String.join(", ", burrowBootstrapServers)
    );

    // normal mode
    Assert.assertEquals(Optional.of(expected), freshness.validateClusterConf(conf));

    // strict mode
    freshness.strict = true;
    Assert.assertEquals(Optional.of(expected), freshness.validateClusterConf(conf));

    Map<String, Object> globalConf = new HashMap<>();
    globalConf.put("clusters", Lists.newArrayList(conf));
    
    // RuntimeException should be thrown when attempting to setup Tracker with invalid configuration in strict mode
    thrown.expect(RuntimeException.class);
    thrown.expectMessage(expected);
    
    freshness.setupWithBurrow(globalConf, burrow);
  }

  Map<String, Object> mockConfForCluster(String name, String... bootstrapServers) {
    Map<String, Object> clusterConf = new HashMap<>();
    Map<String, Object> kafkaConf = new HashMap<>();
    String bootstrapServersString = String.join(",", bootstrapServers);
    kafkaConf.put("bootstrap.servers", bootstrapServersString);
    clusterConf.put("name", name);
    clusterConf.put("kafka", kafkaConf);
    return clusterConf;
  }

  /**
   * Something is quite wrong with the executor, so give up. We assume the executor will just queue outstanding work, so
   * this means something is very wrong, so we should propagate out the exception.
   */
  @Test
  public void testFailToSubmitTaskExitsTracker() throws Exception {
    Burrow burrow = mock(Burrow.class);
    ListeningExecutorService executor = mock(ListeningExecutorService.class);
    ConsumerFreshness freshness = new ConsumerFreshness();
    freshness.setupForTesting(burrow, workers("cluster"), executor);

    Burrow.ClusterClient client = mockClusterState("cluster", "group", partitionState("t", 1, 1, 0));
    when(burrow.getClusters()).thenReturn(newArrayList(client));
    Exception cause = new RejectedExecutionException("injected");
    when(executor.submit(any(Callable.class))).thenThrow(cause);

    thrown.expect(RuntimeException.class);
    thrown.expectCause(org.hamcrest.CoreMatchers.equalTo(cause));
    freshness.run();
  }

  private void assertNoSuccessfulClusterMeasurement(ConsumerFreshness freshness, String cluster){
    FreshnessMetrics metrics = freshness.getMetricsForTesting();
    assertEquals("Cluster measurement should not be successful",
        0.0, metrics.lastClusterRunSuccessfulAttempt.labels(cluster).get(), 0.0);
  }

  private void assertSuccessfulClusterMeasurement(ConsumerFreshness freshness, String cluster){
    FreshnessMetrics metrics = freshness.getMetricsForTesting();
    assertTrue("Cluster measurement should be successful",
        metrics.lastClusterRunSuccessfulAttempt.labels(cluster).get() > 0);
  }

  private void withExecutor(Consumer<ListeningExecutorService> toRun) {
    ListeningExecutorService executor = null;
    try {
      executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(3));
      toRun.accept(executor);
      assertEquals("Should not be any outstanding tasks after a run completes", 0, executor.shutdownNow().size());
    } finally {
      if (executor != null) {
        executor.shutdown();
      }
    }
  }

  private Map<String, ArrayBlockingQueue<KafkaConsumer>> workers(String... clusters) {
    Map<String, ArrayBlockingQueue<KafkaConsumer>> workers = new HashMap<>(clusters.length);
    for (String cluster : clusters) {
      workers.putAll(workers(cluster, mock(KafkaConsumer.class)));
    }
    return workers;
  }

  private Map<String, ArrayBlockingQueue<KafkaConsumer>> workers(String cluster, KafkaConsumer consumer) {
    Map<String, ArrayBlockingQueue<KafkaConsumer>> workers = new HashMap<>(1);
    ArrayBlockingQueue<KafkaConsumer> queue = new ArrayBlockingQueue<>(1);
    queue.add(consumer);
    workers.put(cluster, queue);
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

  /**
   * Create a {@link ConsumerRecords} with a single consumer record at the given topic, partition, offset and lagging
   * by at least the given milliseconds.
   */
  private ConsumerRecords records(String topic, int partition, long offset, long lagMs){
    return new ConsumerRecords(ImmutableMap.of(
        new TopicPartition(topic, partition), asList(
            new ConsumerRecord(topic, partition, offset, System.currentTimeMillis() - lagMs,
                TimestampType.LOG_APPEND_TIME, NULL_CHECKSUM, NULL_SIZE, NULL_SIZE, "key", "value"))
    ));
  }
}
