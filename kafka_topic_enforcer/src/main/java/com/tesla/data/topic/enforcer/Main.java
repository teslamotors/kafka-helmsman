/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.topic.enforcer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

/**
 * The app.
 */
public class Main {

  @Parameter(
      names = {"--help", "-h"},
      help = true)
  private boolean help = false;

  public static void main(String[] args) {
    final Map<String, BaseCommand> commands = new HashMap<>();
    commands.put("validate", new BaseCommand());
    commands.put("dump", new DumpCommand());
    commands.put("enforce", new EnforceCommand());

    final Main main = new Main();
    final JCommander commander = new JCommander(main);
    for (Map.Entry<String, BaseCommand> entry : commands.entrySet()) {
      commander.addCommand(entry.getKey(), entry.getValue());
    }
    commander.parse(args);

    if (main.help || args.length == 0) {
      commander.usage();
      System.exit(BaseCommand.FAILURE);
    }

    final String cmd = commander.getParsedCommand();
    System.exit(commands.get(cmd).run());
  }

}
