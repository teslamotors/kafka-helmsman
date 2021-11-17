/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import com.tesla.data.enforcer.Enforcer;

import org.apache.kafka.common.acl.AclBinding;

import java.util.Collection;
import java.util.List;

public class AclEnforcer extends Enforcer<AclBinding> {
  private final AclService aclService;
  private final boolean dryRun;

  public AclEnforcer(Collection<AclBinding> configured, AclService aclService, boolean safemode, boolean dryRun) {
    super(configured, aclService::listExisting, AclBinding::equals, safemode);
    this.aclService = aclService;
    this.dryRun = dryRun;
  }

  @Override
  protected void create(List<AclBinding> toCreate) {
    if (!dryRun) {
      aclService.create(toCreate);
    }
  }

  @Override
  protected void delete(List<AclBinding> toDelete) {
    if (!dryRun) {
      aclService.delete(toDelete);
    }
  }

}