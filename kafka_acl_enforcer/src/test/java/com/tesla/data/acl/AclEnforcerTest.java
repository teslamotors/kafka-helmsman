/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.kafka.common.acl.AclBinding;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AclEnforcerTest {

  private AclService aclService;
  private AclEnforcer enforcer;
  private List<AclBinding> acls;

  @Before
  public void setup() throws IOException {
    aclService = mock(AclService.class);
    enforcer = new AclEnforcer(emptyList(), aclService, true, true);
    acls = Arrays.asList(AclConfig.bindingsForTest());
  }

  @Test
  public void testCreateDryRun() throws IOException {
    enforcer.create(acls);
    verify(aclService, times(0)).create(anyCollection());
  }

  @Test
  public void testDeleteDryRun() throws IOException {
    enforcer.delete(acls);
    verify(aclService, times(0)).delete(anyCollection());
  }
}
