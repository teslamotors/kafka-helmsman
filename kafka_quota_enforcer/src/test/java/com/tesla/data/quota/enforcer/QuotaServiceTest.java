/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class QuotaServiceTest {

  private AdminZkClient adminClient;

  @Before
  public void setup() {
    adminClient = mock(AdminZkClient.class);
  }

  @Test
  public void testListExisting() {
    // user + client
    Map<String, Properties> existingUserClientQuotas = new HashMap<>();
    Properties user1Client1Props = new Properties();
    user1Client1Props.put("producer_byte_rate", "100");
    existingUserClientQuotas.put("user1/clients/client1", user1Client1Props);

    Properties defaultUserDefaultClient = new Properties();
    defaultUserDefaultClient.put("producer_byte_rate", "200");
    defaultUserDefaultClient.put("consumer_byte_rate", "500");
    defaultUserDefaultClient.put("request_percentage", "80");
    existingUserClientQuotas.put("<default>/clients/<default>", defaultUserDefaultClient);

    // entry with empty props should be ignored (signifies a deleted config)
    existingUserClientQuotas.put("someUser/clients/someClient", new Properties());

    when(adminClient.fetchAllChildEntityConfigs(ConfigType.User(), ConfigType.Client()))
        .thenReturn(JavaConverters.mapAsScalaMapConverter(existingUserClientQuotas).asScala());

    // user
    Map<String, Properties> existingUserQuotas = new HashMap<>();
    Properties user1Props = new Properties();
    user1Props.put("producer_byte_rate", "3000");
    user1Props.put("consumer_byte_rate", "2000");
    existingUserQuotas.put("user1", user1Props);

    Properties user2Props = new Properties();
    user2Props.put("request_percentage", "40");
    existingUserQuotas.put("user2", user2Props);

    Properties defaultUserProps = new Properties();
    defaultUserProps.put("producer_byte_rate", "7000");
    defaultUserProps.put("consumer_byte_rate", "6000");
    existingUserQuotas.put("<default>", defaultUserProps);

    // entry with empty props should be ignored (signifies a deleted config)
    existingUserQuotas.put("someUser", new Properties());

    when(adminClient.fetchAllEntityConfigs(ConfigType.User()))
        .thenReturn(JavaConverters.mapAsScalaMapConverter(existingUserQuotas).asScala());

    // client
    Map<String, Properties> existingClientQuotas = new HashMap<>();
    Properties client1Props = new Properties();
    client1Props.put("producer_byte_rate", "150");
    client1Props.put("request_percentage", "200");
    existingClientQuotas.put("client1", client1Props);

    Properties defaultClientProps = new Properties();
    defaultClientProps.put("producer_byte_rate", "5000");
    existingClientQuotas.put("<default>", defaultClientProps);

    // entry with empty props should be ignored (signifies a deleted config)
    existingUserQuotas.put("someClient", new Properties());

    when(adminClient.fetchAllEntityConfigs(ConfigType.Client()))
        .thenReturn(JavaConverters.mapAsScalaMapConverter(existingClientQuotas).asScala());

    List<ConfiguredQuota> expected = new ArrayList<>();
    expected.add(new ConfiguredQuota("user1", "client1", 100, null, null));
    expected.add(new ConfiguredQuota("<default>", "<default>", 200, 500, 80));
    expected.add(new ConfiguredQuota("user1", null, 3000, 2000, null));
    expected.add(new ConfiguredQuota("user2", null, null, null, 40));
    expected.add(new ConfiguredQuota("<default>", null, 7000, 6000, null));
    expected.add(new ConfiguredQuota(null, "client1", 150, null, 200));
    expected.add(new ConfiguredQuota(null, "<default>", 5000, null, null));

    QuotaService svc = new QuotaService(adminClient);
    Collection<ConfiguredQuota> actual = svc.listExisting();

    Assert.assertTrue(expected.size() == actual.size() && expected.containsAll(actual) && actual.containsAll(expected));
  }

  @Test
  public void testCreateQuotas() {
    List<ConfiguredQuota> toCreate = new ArrayList<>();
    toCreate.add(new ConfiguredQuota("user1", "<default>", 100, null, null));
    toCreate.add(new ConfiguredQuota("user2", null, null, null, 40));
    toCreate.add(new ConfiguredQuota("<default>", null, 7000, 6000, null));
    toCreate.add(new ConfiguredQuota(null, "client1", 150, null, 200));
    toCreate.add(new ConfiguredQuota(null, "<default>", 5000, null, null));

    QuotaService svc = new QuotaService(adminClient);
    svc.create(toCreate);

    Properties user1DefaultClientProps = new Properties();
    user1DefaultClientProps.put("producer_byte_rate", "100");
    Properties user2Props = new Properties();
    user2Props.put("request_percentage", "40");
    Properties defaultUserProps = new Properties();
    defaultUserProps.put("producer_byte_rate", "7000");
    defaultUserProps.put("consumer_byte_rate", "6000");
    Properties client1Props = new Properties();
    client1Props.put("producer_byte_rate", "150");
    client1Props.put("request_percentage", "200");
    Properties defaultClientProps = new Properties();
    defaultClientProps.put("producer_byte_rate", "5000");

    verify(adminClient).changeConfigs(ConfigType.User(), "user1/clients/<default>", user1DefaultClientProps);
    verify(adminClient).changeConfigs(ConfigType.User(), "user2", user2Props);
    verify(adminClient).changeConfigs(ConfigType.User(), "<default>", defaultUserProps);
    verify(adminClient).changeConfigs(ConfigType.Client(), "client1", client1Props);
    verify(adminClient).changeConfigs(ConfigType.Client(), "<default>", defaultClientProps);
    verify(adminClient, times(5)).changeConfigs(any(), any(), any());
  }

  @Test
  public void testDeleteQuotas() {
    List<ConfiguredQuota> toDelete = new ArrayList<>();
    toDelete.add(new ConfiguredQuota("user1", "<default>", 100, null, null));
    toDelete.add(new ConfiguredQuota("user2", null, null, null, 40));
    toDelete.add(new ConfiguredQuota("<default>", null, 7000, 6000, null));
    toDelete.add(new ConfiguredQuota(null, "client1", 150, null, 200));
    toDelete.add(new ConfiguredQuota(null, "<default>", 5000, null, null));

    QuotaService svc = new QuotaService(adminClient);
    svc.delete(toDelete);

    Properties emptyProps = new Properties();

    verify(adminClient).changeConfigs(ConfigType.User(), "user1/clients/<default>", emptyProps);
    verify(adminClient).changeConfigs(ConfigType.User(), "user2", emptyProps);
    verify(adminClient).changeConfigs(ConfigType.User(), "<default>", emptyProps);
    verify(adminClient).changeConfigs(ConfigType.Client(), "client1", emptyProps);
    verify(adminClient).changeConfigs(ConfigType.Client(), "<default>", emptyProps);
    verify(adminClient, times(5)).changeConfigs(any(), any(), any());
  }
}