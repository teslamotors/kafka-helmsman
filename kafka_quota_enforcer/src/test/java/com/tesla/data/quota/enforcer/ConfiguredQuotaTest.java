package com.tesla.data.quota.enforcer;

import com.tesla.data.enforcer.BaseCommand;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class ConfiguredQuotaTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBadConfiguration() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid quota configuration detected. At least one of 'principal' or 'client' must be provided.");
    new ConfiguredQuota(null, null, 5000, 4000, 50d);
  }

  @Test
  public void testConfiguredQuotaEquals() {
    ConfiguredQuota quota = new ConfiguredQuota("user1", "clientA", 2000, 1000, 50d);
    Assert.assertEquals(quota, new ConfiguredQuota("user1", "clientA", 2000, 1000, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("badUser", "clientA", 2000, 1000, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "badClient", 2000, 1000, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 9999, 1000, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 2000, 9999, 50d));
    Assert.assertNotEquals(quota, new ConfiguredQuota("user1", "clientA", 2000, 1000, 99d));
  }

  @Test
  public void testConverter() throws Exception {
    String cfg = String.join("\n",
        "---",
        "kafka:",
        "    bootstrap.servers: localhost:9092",
        "quota:",
        "    - principal: user1",
        "      client: float-client",
        "      producer-byte-rate: 2000.0",
        "      consumer-byte-rate: 1000.1",
        "      request-percentage: 52.7",
        "    - principal: backward-compatible",
        "      client: all-integers",
        "      producer-byte-rate: 2000",
        "      consumer-byte-rate: 1000",
        "      request-percentage: 50");
    final BaseCommand.CommandConfigConverter converter = new BaseCommand.CommandConfigConverter();
    Map<String, Object> config = converter.convert(new ByteArrayInputStream(cfg.getBytes(StandardCharsets.UTF_8)));
    List<ConfiguredQuota> quotas =
        new BaseCommand<ConfiguredQuota>(config).configuredEntities(ConfiguredQuota.class, "quota", "quotaFile");
    Assert.assertEquals(quotas, List.of(
        new ConfiguredQuota("user1", "float-client", 2000, 1000, 52.7),
        new ConfiguredQuota("backward-compatible", "all-integers", 2000, 1000, 50d)));
  }
}
