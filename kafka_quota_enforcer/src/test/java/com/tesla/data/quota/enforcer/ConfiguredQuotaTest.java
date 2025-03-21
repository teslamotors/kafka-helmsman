package com.tesla.data.quota.enforcer;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ConfiguredQuotaTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBadConfiguration() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid quota configuration detected. At least one of 'principal' or 'client' must be provided.");
    new ConfiguredQuota(null, null, 5000d, 4000d, 50d);
  }

  @Test
  public void testConfiguredQuotaEquals() {
    ConfiguredQuota quota = new ConfiguredQuota("user1", "clientA", 2000d, 1000d, 50d);
    Assert.assertEquals(quota, new ConfiguredQuota("user1", "clientA", 2000d, 1000d, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("badUser", "clientA", 2000d, 1000d, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "badClient", 2000d, 1000d, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 9999d, 1000d, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 2000d, 9999d, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 2000d, 1000d, 99d));
  }
}
