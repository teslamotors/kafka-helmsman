package com.tesla.data.quota.enforcer;

import com.tesla.data.enforcer.BaseCommand;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

import java.util.HashMap;
import java.util.Map;

public class Main {
  @Parameter(
      names = {"--help", "h"},
      help = true)
  private boolean help = false;

  public static void main(String[] args) {
    final Main main = new Main();
    final Map<String, BaseCommand<ConfiguredQuota>> commands = new HashMap<>();
    commands.put("validate", new BaseCommand<>());
    commands.put("enforce", new QuotaEnforceCommand());

    final JCommander commander = new JCommander(main);
    for (Map.Entry<String, BaseCommand<ConfiguredQuota>> entry : commands.entrySet()) {
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
