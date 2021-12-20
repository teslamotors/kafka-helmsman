/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import com.tesla.data.enforcer.EnforceCommand;
import com.tesla.data.enforcer.Enforcer;

import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.kafka.common.utils.Time;

import java.util.Map;


public class QuotaEnforceCommand extends EnforceCommand<ConfiguredQuota> {
  private KafkaZkClient zkClient;
  // These are defaults used by the ZK admin client, "preserved by default for compatibility with previous versions"
  private static final String ZK_METRIC_GROUP = "kafka.server";
  private static final String ZK_METRIC_TYPE = "SessionExpireListener";

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
    Map<String, Object> zkConfig = zookeeperConfig();
    this.zkClient = KafkaZkClient.apply(
        zkConfig.get("connect").toString(),
        JaasUtils.isZkSecurityEnabled(),
        Integer.parseInt(zkConfig.get("sessionTimeoutMs").toString()),
        Integer.parseInt(zkConfig.get("connectionTimeoutMs").toString()),
        Integer.MAX_VALUE,
        Time.SYSTEM,
        ZK_METRIC_GROUP,
        ZK_METRIC_TYPE,
        scala.Option.empty()
    );
    AdminZkClient adminClient = new AdminZkClient(zkClient);
    return new QuotaEnforcer(configuredEntities(ConfiguredQuota.class, "quotas", "quotasFile"),
        new QuotaService(adminClient), !unsafemode, dryrun);
  }

  @Override
  protected void close() {
    if (this.zkClient != null) {
      this.zkClient.close();
    }
  }
}
