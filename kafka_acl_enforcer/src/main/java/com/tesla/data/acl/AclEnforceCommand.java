/*
 * Copyright © 2020 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import com.tesla.data.acl.mixin.Json;
import com.tesla.data.enforcer.EnforceCommand;
import com.tesla.data.enforcer.Enforcer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.acl.AclBinding;

import java.util.Map;

public class AclEnforceCommand extends EnforceCommand<AclBinding> {
  private AdminClient adminClient;
  static {
    Json.addMixIns(MAPPER);
  }

  public AclEnforceCommand() {
    // DO NOT REMOVE, this is needed by jcommander
  }

  public AclEnforceCommand(Map<String, Object> cmdConfig) {
    this(cmdConfig, null);
  }

  public AclEnforceCommand(Map<String, Object> cmdConfig, String cluster) {
    this.cmdConfig = cmdConfig;
    this.cluster = cluster;
  }

  @Override
  protected Enforcer<AclBinding> initEnforcer() {
    this.adminClient = KafkaAdminClient.create(kafkaConfig());
    return new AclEnforcer(configuredEntities(AclBinding.class, "acls", "aclsFile"),
        new AclService(adminClient), !unsafemode, dryrun);
  }

  @Override
  protected void close() {
    if (this.adminClient != null) {
      this.adminClient.close();
    }
  }
}
