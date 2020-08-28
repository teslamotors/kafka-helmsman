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

  public AclEnforcer(Collection<AclBinding> configured, AclService aclService, boolean safemode) {
    super(configured, aclService::listExisting, AclBinding::equals, safemode);
    this.aclService = aclService;
  }

  @Override
  protected void create(List<AclBinding> toCreate) {
    aclService.create(toCreate);
  }

  @Override
  protected void delete(List<AclBinding> toDelete) {
    aclService.delete(toDelete);
  }
}
