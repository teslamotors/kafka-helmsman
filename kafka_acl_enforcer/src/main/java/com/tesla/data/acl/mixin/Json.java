/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl.mixin;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;

/** JSON utilities for ACL structures */
public class Json {

  /** Add mixins for {@link org.apache.kafka.common.acl.AclBinding} json sede. */
  public static void addMixIns(ObjectMapper mapper) {
    mapper.addMixIn(AclBinding.class, AclBindingMixin.class);
    mapper.addMixIn(AccessControlEntry.class, AccessControlEntryMixin.class);
    mapper.addMixIn(ResourcePattern.class, ResourcePatternMixin.class);
  }
}
