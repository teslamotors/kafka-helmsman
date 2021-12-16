/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import kafka.server.ConfigType;
import kafka.zk.AdminZkClient;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

public class QuotaService {
  private final AdminZkClient adminClient;
  private static final String PRODUCER_BYTE_RATE = "producer_byte_rate";
  private static final String CONSUMER_BYTE_RATE = "consumer_byte_rate";
  private static final String REQUEST_PERCENTAGE = "request_percentage";

  public QuotaService(AdminZkClient adminClient) {
    this.adminClient = adminClient;
  }

  enum EntityType {
    // quota applied solely on a per principal basis, ex: principal=user1
    PRINCIPAL,
    // quota applied solely on a per client basis, ex: client=clientA
    CLIENT,
    // quota applied on a combination of principal AND client, ex: (principal=user1, client=clientA)
    PRINCIPAL_AND_CLIENT
  }

  /**
   * Build and return a collection of {@link ConfiguredQuota} that currently exist. Drops any returned quota entry
   * that contains an empty configuration/property map -- these signify that the quota had been previously deleted.
   */
  public Collection<ConfiguredQuota> listExisting() {
    // Get all existing quotas that are applied to users AND clients, for ex: principal=<default>, client=clientA
    Map<String, Properties> existingQuotasUserClient = JavaConverters.mapAsJavaMapConverter(
        adminClient.fetchAllChildEntityConfigs(ConfigType.User(), ConfigType.Client())).asJava();
    Set<ConfiguredQuota> existing = existingQuotasUserClient.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .map(e -> configuredQuota(e, EntityType.PRINCIPAL_AND_CLIENT))
        .collect(Collectors.toSet());

    // Get all existing quotas that are applied ONLY to users, for ex: principal=user1
    Map<String, Properties> existingQuotasUser = JavaConverters.mapAsJavaMapConverter(
        adminClient.fetchAllEntityConfigs(ConfigType.User())).asJava();
    existing.addAll(existingQuotasUser.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .map(e -> configuredQuota(e, EntityType.PRINCIPAL))
        .collect(Collectors.toSet()));

    // Get all existing quotas that are applied ONLY to clients, for ex: client=<default>
    Map<String, Properties> existingQuotasClient = JavaConverters.mapAsJavaMapConverter(
        adminClient.fetchAllEntityConfigs(ConfigType.Client())).asJava();
    existing.addAll(existingQuotasClient.entrySet().stream()
        .filter(e -> !e.getValue().isEmpty())
        .map(e -> configuredQuota(e, EntityType.CLIENT))
        .collect(Collectors.toSet()));

    return existing;
  }

  public void create(Collection<ConfiguredQuota> quotas) {
    for (ConfiguredQuota quota : quotas) {
      String entityType = quota.getPrincipal() != null ? ConfigType.User() : ConfigType.Client();
      String entityName = getEntityName(entityType, quota.getPrincipal(), quota.getClient());

      adminClient.changeConfigs(entityType, entityName, quotaProperties(quota));
    }
  }

  public void delete(Collection<ConfiguredQuota> quotas) {
    for (ConfiguredQuota quota : quotas) {
      String entityType = quota.getPrincipal() != null ? ConfigType.User() : ConfigType.Client();
      String entityName = getEntityName(entityType, quota.getPrincipal(), quota.getClient());

      adminClient.changeConfigs(entityType, entityName, new Properties());
    }
  }

  private ConfiguredQuota configuredQuota(Map.Entry<String, Properties> quotaEntry, EntityType entityType) {
    Properties quota = quotaEntry.getValue();
    Integer producerByteRate = getQuotaValue(quota, PRODUCER_BYTE_RATE);
    Integer consumerByteRate = getQuotaValue(quota, CONSUMER_BYTE_RATE);
    Integer requestPercentage = getQuotaValue(quota, REQUEST_PERCENTAGE);

    switch (entityType) {
      case PRINCIPAL:
        return new ConfiguredQuota(quotaEntry.getKey(), null, producerByteRate, consumerByteRate, requestPercentage);
      case CLIENT:
        return new ConfiguredQuota(null, quotaEntry.getKey(), producerByteRate, consumerByteRate, requestPercentage);
      case PRINCIPAL_AND_CLIENT:
        // when both user + client are configured, the full name looks like {configuredUser}/clients/{configuredClient}
        Matcher matcher = Pattern.compile("(?<principal>.*)/clients/(?<client>.*)").matcher(quotaEntry.getKey());
        if (!matcher.find()) {
          throw new RuntimeException("Could not recognize entity name pattern: " + quotaEntry.getKey());
        }
        return new ConfiguredQuota(matcher.group("principal"), matcher.group("client"), producerByteRate,
            consumerByteRate, requestPercentage);
      default:
        throw new RuntimeException("Quota type not recognized: " + entityType);
    }
  }

  private Properties quotaProperties(ConfiguredQuota quota) {
    Properties props = new Properties();
    if (quota.getProducerByteRate() != null) {
      props.put(PRODUCER_BYTE_RATE, String.valueOf(quota.getProducerByteRate()));
    }
    if (quota.getConsumerByteRate() != null) {
      props.put(CONSUMER_BYTE_RATE, String.valueOf(quota.getConsumerByteRate()));
    }
    if (quota.getRequestPercentage() != null) {
      props.put(REQUEST_PERCENTAGE, String.valueOf(quota.getRequestPercentage()));
    }
    return props;
  }

  /**
   * Return the full entity name based on the entity type. If only the principal or client is specified, the entity
   * name is simply that string. If both are specified, then the entity name is: principal + "/clients/" + client
   */
  private String getEntityName(String entityType, String principal, String client) {
    String entityName;
    if (entityType.equals(ConfigType.User())) {
      entityName = principal;
      if (client != null) {
        entityName += "/clients/" + client;
      }
    } else {
      entityName = client;
    }
    return entityName;
  }

  /**
   * Return the quota value if it exists in the properties, or null if it does not exist.
   */
  private Integer getQuotaValue(Properties quota, String quotaType) {
    return quota.get(quotaType) != null ? Integer.valueOf(quota.getProperty(quotaType)) : null;
  }
}
