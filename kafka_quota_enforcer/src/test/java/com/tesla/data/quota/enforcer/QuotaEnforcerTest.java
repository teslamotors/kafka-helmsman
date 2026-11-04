/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
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

  @Test
  public void testValueDriftedQuotaIsNotUnexpected() {
    // same entity as the configured quota, but with different values
    ConfiguredQuota drifted = new ConfiguredQuota("user1", "clientA", 9999d, 4000d, 100d);
    when(quotaService.listExisting()).thenReturn(Arrays.asList(drifted));
    QuotaEnforcer enforcer = new QuotaEnforcer(Arrays.asList(quotas.get(0)), quotaService, false, false);

    // the entity is configured, so its drifted quota must not be scheduled for deletion; the
    // configured quota is absent (no value-equal existing quota), so create will overwrite it
    assertTrue("a quota with drifted values must not be deleted", enforcer.unexpected().isEmpty());
    assertEquals(Arrays.asList(quotas.get(0)), enforcer.absent());
  }

  @Test
  public void testUnconfiguredEntityIsUnexpected() {
    ConfiguredQuota unconfigured = new ConfiguredQuota("user2", "clientB", 1000d, null, null);
    when(quotaService.listExisting()).thenReturn(Arrays.asList(unconfigured));
    QuotaEnforcer enforcer = new QuotaEnforcer(Arrays.asList(quotas.get(0)), quotaService, false, false);

    assertEquals(Arrays.asList(unconfigured), enforcer.unexpected());
  }

  @Test
  public void testEnforceAllDoesNotDeleteAlteredQuota() {
    // an entity whose quota values changed in config: enforceAll must recreate it with the new
    // values and must not delete the entity afterwards (that would clear the quota it just applied).
    // Include a second unchanged configured entity so that if any unexpected entities were detected,
    // the destruction ratio would be 1/2 = 50% (under PERMISSIBLE_DELETION_THRESHOLD of 50%).
    ConfiguredQuota drifted = new ConfiguredQuota("user1", "clientA", 9999d, 4000d, 100d);
    ConfiguredQuota unchanged = quotas.get(1);
    when(quotaService.listExisting()).thenReturn(Arrays.asList(drifted, unchanged));
    QuotaEnforcer enforcer = new QuotaEnforcer(quotas, quotaService, false, false);

    enforcer.enforceAll();

    verify(quotaService).create(Arrays.asList(quotas.get(0)));
    verify(quotaService, never()).delete(any(Collection.class));
  }
}
