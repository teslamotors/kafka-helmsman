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

  @Test
  public void testConverter() throws Exception {
    String cfg = String.join("\n",
        "---",
        "kafka:",
        "    bootstrap.servers: localhost:9092",
        "quotas:",
        "    - principal: user1",
        "      client: float-client",
        "      producer-byte-rate: 2000.0",
        "      consumer-byte-rate: 1000.0",
        "      request-percentage: 52.7",
        "    - principal: backward-compatible",
        "      client: all-integers",
        "      producer-byte-rate: 2000",
        "      consumer-byte-rate: 1000",
        "      request-percentage: 50");
    final BaseCommand.CommandConfigConverter converter = new BaseCommand.CommandConfigConverter();
    Map<String, Object> config = converter.convert(new ByteArrayInputStream(cfg.getBytes(StandardCharsets.UTF_8)));
    List<ConfiguredQuota> quotas =
        new BaseCommand<ConfiguredQuota>(config).configuredEntities(ConfiguredQuota.class, "quotas", "quotasFile");
    Assert.assertEquals(quotas, List.of(
        new ConfiguredQuota("user1", "float-client", 2000d, 1000d, 52.7),
        new ConfiguredQuota("backward-compatible", "all-integers", 2000d, 1000d, 50d)));
  }
}
