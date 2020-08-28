/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import com.tesla.data.acl.mixin.Json;
import com.tesla.data.enforcer.BaseCommand;
import com.tesla.data.enforcer.EnforceCommand;
import com.tesla.data.enforcer.Enforcer;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;

import java.util.HashMap;
import java.util.Map;

public class Main {

  private static class AclEnforceCommand extends EnforceCommand<AclBinding> {
    @Override
    protected Enforcer<AclBinding> initEnforcer(AdminClient client) {
      Json.addMixIns(MAPPER);
      return new AclEnforcer(configuredEntities(AclBinding.class, "acls", "aclsFile"),
          new AclServiceImpl(client, dryrun), !unsafemode);
    }
  }

  @Parameter(
      names = {"--help", "-h"},
      help = true)
  private boolean help = false;

  public static void main(String[] args) {
    final Main main = new Main();

    final Map<String, BaseCommand<AclBinding>> commands = new HashMap<>();
    commands.put("validate", new BaseCommand<>());
    commands.put("enforce", new AclEnforceCommand());

    final JCommander commander = new JCommander(main);
    for (Map.Entry<String, BaseCommand<AclBinding>> entry : commands.entrySet()) {
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
