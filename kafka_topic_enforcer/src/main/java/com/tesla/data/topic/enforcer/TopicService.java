/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * All kafka interactions go through TopicService.
 */
public interface TopicService {

  // The internal topic check, '_foo' is an internal topic, 'bar' is not.
  Predicate<ConfiguredTopic> INTERNAL_TOPIC = x -> x.getName().startsWith("_");

  /**
   * Load existing topics from a kafka cluster.
   *
   * @param excludeInternal if true kafka internal topics are excluded
   * @return a collection of topics.
   */
  Map<String, ConfiguredTopic> listExisting(boolean excludeInternal);

  /**
   * Create new topics.
   *
   * @param topics a collections of topics
   */
  void create(List<ConfiguredTopic> topics);

  /**
   * Alter topics by increasing partition count as defined by each individual @link ConfiguredTopic} instance.
   *
   * @param topics a collections of topics
   */
  void increasePartitions(List<ConfiguredTopic> topics);

  /**
   * Alter topics by applying the configuration as defined by each individual @link ConfiguredTopic} instance.
   *
   * @param topics a collections of topics
   */
  void alterConfiguration(List<ConfiguredTopic> topics);

  /**
   * Delete topics.
   *
   * @param topics a collection of topics
   */
  void delete(List<ConfiguredTopic> topics);
}
