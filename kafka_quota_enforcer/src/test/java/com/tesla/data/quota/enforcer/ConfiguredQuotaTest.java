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
    new ConfiguredQuota(null, null, 5000, 4000, 50);
  }

  @Test
  public void testConfiguredQuotaEquals() {
    ConfiguredQuota quota = new ConfiguredQuota("user1", "clientA", 2000, 1000, 50);
    Assert.assertEquals(quota, new ConfiguredQuota("user1", "clientA", 2000, 1000, 50));
    Assert.assertNotEquals(quota, new ConfiguredQuota("badUser", "clientA", 2000, 1000, 50));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "badClient", 2000, 1000, 50));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 9999, 1000, 50));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 2000, 9999, 50));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 2000, 1000, 99));
  }
}
