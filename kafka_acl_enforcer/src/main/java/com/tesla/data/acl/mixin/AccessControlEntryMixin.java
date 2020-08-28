/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;

public abstract class AccessControlEntryMixin {

  @JsonCreator
  public AccessControlEntryMixin(
      @JsonProperty("principal") String principal,
      @JsonProperty("host") String host,
      @JsonProperty("operation") AclOperation operation,
      @JsonProperty("permission") AclPermissionType permissionType) {
    // for jackson
  }
}
