/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import com.tesla.data.enforcer.BaseCommand;

import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import java.util.Collection;
import java.util.Comparator;
import java.util.stream.Collectors;

@Parameters(commandDescription = "Dump existing cluster config on stdout. " +
    "A subset of topic config is not serialized on the server side and that would be missing in " +
    "the dump output, example: topic tags.")
public class DumpCommand extends BaseCommand<ConfiguredTopic> {

  @Override
  public int run() {
    try (AdminClient kafka = KafkaAdminClient.create(kafkaConfig())) {
      TopicService topicService = new TopicServiceImpl(kafka, true);
      Collection<ConfiguredTopic> existing = topicService.listExisting().values()
          .stream()
          .sorted(Comparator.comparing(ConfiguredTopic::getName))
          .collect(Collectors.toList());
      System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(existing));
      return SUCCESS;
    } catch (JsonProcessingException e) {
      LOG.error("Failed to dump config", e);
      return FAILURE;
    }
  }

}
