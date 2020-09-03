/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.resource.ResourcePatternFilter;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class AclService {

  private final AdminClient adminClient;

  public AclService(AdminClient adminClient) {
    this.adminClient = adminClient;
  }

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

  public void create(Collection<AclBinding> acls) {

    try {
      adminClient.createAcls(acls).all().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }

  }

  public void delete(Collection<AclBinding> acls) {
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
