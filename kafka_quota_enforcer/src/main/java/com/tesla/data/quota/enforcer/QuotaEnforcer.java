/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */


package com.tesla.data.quota.enforcer;

import com.tesla.data.enforcer.Enforcer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class QuotaEnforcer extends Enforcer<ConfiguredQuota> {
  private final AdminClientQuotaService quotaService;
  private final boolean dryRun;

  public QuotaEnforcer(Collection<ConfiguredQuota> configuredQuotas, AdminClientQuotaService quotaService,
                       boolean safeMode, boolean dryRun) {
    super(configuredQuotas, quotaService::listExisting, ConfiguredQuota::equals, safeMode);
    this.quotaService = quotaService;
    this.dryRun = dryRun;
  }

  /**
   * Get quotas which exist in the cluster for entities that are not configured at all. Unlike the base
   * implementation, entities are matched on identity (principal and client) rather than on full quota values.
   *
   * <p>Quotas for the same entity share a single key on the broker. When a configured quota's values change,
   * the configured quota is 'absent' (there is no existing quota with equal values) and the existing quota is,
   * by value comparison, 'unexpected'. Creating the former and then deleting the latter both address the same
   * broker entity, so the delete would clear the quota that was just applied, leaving the entity without any
   * limits until the next enforcement run recreates it. Matching on entity identity here means a value change
   * is handled by create alone (which overwrites the entity's quotas), and deletion only applies to entities
   * that have been removed from the configuration.
   */
  @Override
  public List<ConfiguredQuota> unexpected() {
    return Collections.unmodifiableList(
        this.existing.get().stream()
            .filter(e -> this.configured.stream().noneMatch(c -> sameEntity(c, e)))
            .collect(Collectors.toList()));
  }

  private static boolean sameEntity(ConfiguredQuota a, ConfiguredQuota b) {
    return Objects.equals(a.getPrincipal(), b.getPrincipal()) && Objects.equals(a.getClient(), b.getClient());
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
