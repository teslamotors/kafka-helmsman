/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */


package com.tesla.data.quota.enforcer;

import com.tesla.data.enforcer.Enforcer;

import java.util.Collection;
import java.util.List;

public class QuotaEnforcer extends Enforcer<ConfiguredQuota> {
  private final QuotaService quotaService;
  private final boolean dryRun;

  public QuotaEnforcer(Collection<ConfiguredQuota> configuredQuotas, QuotaService quotaService,
                       boolean safeMode, boolean dryRun) {
    super(configuredQuotas, quotaService::listExisting, ConfiguredQuota::equals, safeMode);
    this.quotaService = quotaService;
    this.dryRun = dryRun;
  }

  @Override
  protected void create(List<ConfiguredQuota> toCreate) {
    if (!dryRun) {
      quotaService.create(toCreate);
    }
  }

  @Override
  protected void delete(List<ConfiguredQuota> toDelete) {
    if (!dryRun) {
      quotaService.delete(toDelete);
    }
  }
}
