/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.resource.ResourcePattern;

public abstract class AclBindingMixin {
  @JsonCreator
  AclBindingMixin(@JsonProperty("resource") ResourcePattern pattern, @JsonProperty("entry") AccessControlEntry entry) {
    // for jackson
  }
}