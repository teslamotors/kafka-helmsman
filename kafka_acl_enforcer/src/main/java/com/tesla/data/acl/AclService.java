/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import org.apache.kafka.common.acl.AclBinding;

import java.util.Collection;

public interface AclService {

  /**
   * List existing acls.
   */
  Collection<AclBinding> listExisting();

  /**
   * Create ACLs.
   */
  void create(Collection<AclBinding> acls);

  /**
   * Delete ACLs.
   */
  void delete(Collection<AclBinding> acls);

}

