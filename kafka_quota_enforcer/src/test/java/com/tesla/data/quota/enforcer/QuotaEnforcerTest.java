/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class QuotaEnforcerTest {

  private AdminClientQuotaService quotaService;
  private static final List<ConfiguredQuota> quotas = Arrays.asList(
      new ConfiguredQuota("user1", "clientA", 5000d, 4000d, 100d),
      new ConfiguredQuota("<default>", null, 6000d, 3000d, null)
  );

  @Before
  public void setup() {
    quotaService = mock(AdminClientQuotaService.class);
  }

  @Test
  public void testCreate() {
    QuotaEnforcer enforcer = new QuotaEnforcer(emptyList(), quotaService, true, false);
    enforcer.create(quotas);
    verify(quotaService).create(quotas);
  }

  @Test
  public void testDelete() {
    QuotaEnforcer enforcer = new QuotaEnforcer(emptyList(), quotaService, true, false);
    enforcer.delete(quotas);
    verify(quotaService).delete(quotas);
  }

  @Test
  public void testCreateDryRun() {
    QuotaEnforcer enforcer = new QuotaEnforcer(emptyList(), quotaService, true, true);
    enforcer.create(quotas);
    verifyNoInteractions(quotaService);
  }

  @Test
  public void testDeleteDryRun() {
    QuotaEnforcer enforcer = new QuotaEnforcer(emptyList(), quotaService, true, true);
    enforcer.delete(quotas);
    verifyNoInteractions(quotaService);
  }
}
