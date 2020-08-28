/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import static org.mockito.Matchers.anyCollectionOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

public class AclServiceImplTest {

  private AdminClient client;

  @Before
  public void setup() {
    client = mock(AdminClient.class);
  }

  @Test
  public void testCreateDryRun() throws IOException {
    AclService service = new AclServiceImpl(client, true);
    service.create(Arrays.asList(AclConfig.bindingsForTest()));
    verify(client, times(0)).createAcls(anyCollectionOf(AclBinding.class));
  }

  @Test
  public void testDeleteDryRun() throws IOException {
    AclService service = new AclServiceImpl(client, true);
    service.create(Arrays.asList(AclConfig.bindingsForTest()));
    verify(client, times(0)).deleteAcls(anyCollectionOf(AclBindingFilter.class));
  }

}