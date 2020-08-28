/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.ResourcePatternFilter;

public class AclServiceImpl implements AclService {

  private final AdminClient adminClient;
  private final boolean dryRun;

  public AclServiceImpl(AdminClient adminClient, boolean dryRun) {
    this.adminClient = adminClient;
    this.dryRun = dryRun;
  }

  @Override
  public Collection<AclBinding> listExisting() {
    try {
      return adminClient
          .describeAcls(
              new AclBindingFilter(ResourcePatternFilter.ANY, AccessControlEntryFilter.ANY))
          .values()
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void create(Collection<AclBinding> acls) {
    if (!dryRun) {
      try {
        adminClient.createAcls(acls).all().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void delete(Collection<AclBinding> acls) {
    if (!dryRun) {
      try {
        List<AclBindingFilter> filters =
            acls.stream()
                .map(
                    a ->
                        new AclBindingFilter(
                            new ResourcePatternFilter(
                                a.pattern().resourceType(),
                                a.pattern().name(),
                                a.pattern().patternType()),
                            new AccessControlEntryFilter(
                                a.entry().principal(),
                                a.entry().host(),
                                a.entry().operation(),
                                a.entry().permissionType())))
                .collect(Collectors.toList());
        adminClient.deleteAcls(filters).all().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
