/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import com.tesla.data.enforcer.EnforceCommand;
import com.tesla.data.enforcer.Enforcer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;

import java.util.Map;


public class QuotaEnforceCommand extends EnforceCommand<ConfiguredQuota> {
  private AdminClient adminClient;

  public QuotaEnforceCommand(){
    // DO NOT REMOVE, this is needed by jcommander
  }

  // for testing
  public QuotaEnforceCommand(Map<String, Object> cmdConfig, String cluster) {
    this.cmdConfig = cmdConfig;
    this.cluster = cluster;
  }

  @Override
  protected Enforcer<ConfiguredQuota> initEnforcer() {
    this.adminClient = KafkaAdminClient.create(kafkaConfig());
    return new QuotaEnforcer(configuredEntities(ConfiguredQuota.class, "quotas", "quotasFile"),
        new AdminClientQuotaService(adminClient), !unsafemode, dryrun);
  }

  @Override
  protected void close() {
    if (this.adminClient != null) {
      this.adminClient.close();
    }
  }
}
