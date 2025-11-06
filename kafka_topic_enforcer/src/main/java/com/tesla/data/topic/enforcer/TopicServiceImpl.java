/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigsOptions;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreatePartitionsOptions;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class TopicServiceImpl implements TopicService {

  private static final ListTopicsOptions EXCLUDE_INTERNAL = new ListTopicsOptions().listInternal(false);
  private static final int MAX_PARTITIONS_PER_CREATE_BATCH = 10_000;

  private final AdminClient adminClient;
  private final boolean dryRun;

  public TopicServiceImpl(AdminClient adminClient, boolean dryRun) {
    this.adminClient = adminClient;
    this.dryRun = dryRun;
  }

  /**
   * Transform a TopicDescription instance to ConfiguredTopic instance.
   *
   * @param td  an instance of TopicDescription
   * @param ktc a topic config future
   * @return an instance of ConfiguredTopic
   */
  static ConfiguredTopic configuredTopic(TopicDescription td, KafkaFuture<Config> ktc) {
    int partitions = td.partitions().size();
    short replication = (short) td.partitions().iterator().next().replicas().size();
    try {
      Config tc = ktc.get();
      Map<String, String> configMap = tc
          .entries()
          .stream()
          .filter(TopicServiceImpl::isNonDefault)
          .collect(toMap(ConfigEntry::name, ConfigEntry::value));
      return new ConfiguredTopic(td.name(), partitions, replication, configMap);
    } catch (InterruptedException | ExecutionException e) {
      // TODO: FA-10109: Improve exception handling
      throw new RuntimeException(e);
    }
  }

  /**
   * Transform free form topic config to a {@link Config} instance.
   *
   * @param topic an instance of {@link ConfiguredTopic}
   * @return an instance of {@link Config}
   */
  static Config resourceConfig(ConfiguredTopic topic) {
    return new Config(topic.getConfig()
        .entrySet()
        .stream()
        .map(e -> new ConfigEntry(e.getKey(), e.getValue()))
        .collect(toList()));
  }

  /**
   * Check if the given topic configuration is set to default or otherwise.
   *
   * @param e a config entry
   * @return true if config is not set to its default value
   */
  static boolean isNonDefault(ConfigEntry e) {
    // DYNAMIC_TOPIC_CONFIG is a config that is configured for a specific topic.
    // For per topic config enforcement, that is the only type which interest
    // us since that indicates an override. All other ConfigSource types are
    // either default provided by kafka or default set by the cluster admin in
    // broker properties.
    return e.source().equals(ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG);
  }

  @Override
  public Map<String, ConfiguredTopic> listExisting() {
    try {
      Set<String> topics = adminClient.listTopics(EXCLUDE_INTERNAL).names().get();
      Collection<TopicDescription> topicDescriptions = adminClient.describeTopics(topics).all().get().values();

      List<ConfigResource> resources = topics
          .stream()
          .map(t -> new ConfigResource(Type.TOPIC, t))
          .collect(toList());

      Map<ConfigResource, KafkaFuture<Config>> topicConfigs = adminClient.describeConfigs(resources).values();

      return topicDescriptions
          .stream()
          .map(td -> configuredTopic(td, topicConfigs.get(new ConfigResource(Type.TOPIC, td.name()))))
          .filter(t -> !INTERNAL_TOPIC.test(t))
          .collect(toMap(ConfiguredTopic::getName, td -> td));

    } catch (InterruptedException | ExecutionException e) {
      // TODO: FA-10109: Improve exception handling
      throw new RuntimeException(e);
    }
  }

  // Kafka Admin API limits the total partitions for the topics that can be created in each API call. This helper
  // function splits the topics into batches to create so each API call is within the limit.
  private void createInBatches(List<NewTopic> topics, CreateTopicsOptions options, int maxPartitionsPerBatch)
      throws ExecutionException, InterruptedException {
    int start = 0;
    int partitionSum = 0;
    for (int i = 0; i < topics.size(); i++) {
      partitionSum += topics.get(i).numPartitions();
      if (partitionSum <= maxPartitionsPerBatch) {
        continue;
      }

      // Create the topics in the last batch if the current topic makes the partition sum exceed the limit.
      adminClient.createTopics(topics.subList(start, i), options).all().get();

      start = i;
      partitionSum = topics.get(i).numPartitions();
    }
    // Create the very last batch of topics.
    adminClient.createTopics(topics.subList(start, topics.size()), options).all().get();
  }

  @Override
  public void create(List<ConfiguredTopic> topics) {
    try {
      CreateTopicsOptions options = dryRun ? new CreateTopicsOptions().validateOnly(true) : new CreateTopicsOptions();
      List<NewTopic> newTopics = topics
          .stream()
          .map(t -> new NewTopic(t.getName(), t.getPartitions(), t.getReplicationFactor()).configs(t.getConfig()))
          .collect(toList());
      createInBatches(newTopics, options, MAX_PARTITIONS_PER_CREATE_BATCH);
    } catch (InterruptedException | ExecutionException e) {
      // TODO: FA-10109: Improve exception handling
      throw new RuntimeException(e);
    }
  }

  @Override
  public void increasePartitions(List<ConfiguredTopic> topics) {
    CreatePartitionsOptions options = dryRun ? new CreatePartitionsOptions().validateOnly(true) :
        new CreatePartitionsOptions();
    Map<String, NewPartitions> newPartitions = topics
        .stream()
        .collect(toMap(ConfiguredTopic::getName, t -> NewPartitions.increaseTo(t.getPartitions())));
    try {
      adminClient.createPartitions(newPartitions, options).all().get();
    } catch (InterruptedException | ExecutionException e) {
      // TODO: FA-10109: Improve exception handling
      throw new RuntimeException(e);
    }
  }

  @Override
  public void alterConfiguration(List<ConfiguredTopic> topics) {
    AlterConfigsOptions options = dryRun ? new AlterConfigsOptions().validateOnly(true) : new AlterConfigsOptions();
    Map<ConfigResource, Config> configs = topics
        .stream()
        .collect(toMap(t -> new ConfigResource(Type.TOPIC, t.getName()), TopicServiceImpl::resourceConfig));
    try {
      adminClient.alterConfigs(configs, options).all().get();
    } catch (InterruptedException | ExecutionException e) {
      // TODO: FA-10109: Improve exception handling
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(List<ConfiguredTopic> topics) {
    // delete topic options don't have a validate only flag
    if (!dryRun) {
      List<String> names = topics
          .stream()
          .map(ConfiguredTopic::getName)
          .collect(toList());
      adminClient.deleteTopics(names);
    }
  }

}
