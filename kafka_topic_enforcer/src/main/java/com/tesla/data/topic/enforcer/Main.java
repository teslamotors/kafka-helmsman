/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import com.tesla.data.enforcer.BaseCommand;
import com.tesla.data.enforcer.EnforceCommand;
import com.tesla.data.enforcer.Enforcer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.HashMap;
import java.util.Map;

/**
 * The app.
 */
public class Main extends EnforceCommand<ConfiguredTopic> {

  @Parameter(
      names = {"--help", "-h"},
      help = true)
  private boolean help = false;

  public static void main(String[] args) {
    final Main main = new Main();
    final Map<String, BaseCommand<ConfiguredTopic>> commands = new HashMap<>();
    commands.put("validate", new BaseCommand<>());
    commands.put("dump", new DumpCommand());
    commands.put("enforce", main);

    final JCommander commander = new JCommander(main);
    commands.forEach(commander::addCommand);
    commander.parse(args);

    if (main.help || args.length == 0) {
      commander.usage();
      System.exit(BaseCommand.FAILURE);
    }

    final String cmd = commander.getParsedCommand();
    System.exit(commands.get(cmd).run());
  }

  @Override
  protected Enforcer<ConfiguredTopic> initEnforcer(AdminClient adminClient) {
    return new TopicEnforcer(new TopicServiceImpl(adminClient, dryrun),
        configuredEntities(ConfiguredTopic.class, "topics", "topicsFile"), !unsafemode);
  }

}
