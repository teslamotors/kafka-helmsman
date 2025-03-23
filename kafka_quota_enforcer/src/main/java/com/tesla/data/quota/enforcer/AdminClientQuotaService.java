/*
 * Copyright (c) 2025. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class AdminClientQuotaService {
  protected final Logger LOG = LoggerFactory.getLogger(getClass());

  private static final String PRODUCER_BYTE_RATE = "producer_byte_rate";
  private static final String CONSUMER_BYTE_RATE = "consumer_byte_rate";
  private static final String REQUEST_PERCENTAGE = "request_percentage";

  private static final List<String> SUPPORTED_QUOTA_TYPES = List.of(PRODUCER_BYTE_RATE, CONSUMER_BYTE_RATE, REQUEST_PERCENTAGE);
  private static final List<String> SUPPORTED_ENTITY_KEYS = List.of(ClientQuotaEntity.USER, ClientQuotaEntity.CLIENT_ID);

  private static final Collection<ClientQuotaAlteration.Op> CLEAR_ALL = SUPPORTED_QUOTA_TYPES.stream().map(t ->
      new ClientQuotaAlteration.Op(t, null)).collect(Collectors.toUnmodifiableList());

  private final AdminClient adminClient;

  public AdminClientQuotaService(AdminClient adminClient) {
    this.adminClient = adminClient;
  }

  /**
   * Build and return a collection of {@link ConfiguredQuota} that currently exist. Drops any returned quota entry
   * that contains an empty configuration/property map -- these signify that the quota had been previously deleted.
   */
  public Collection<ConfiguredQuota> listExisting() {
    try {
      Map<ClientQuotaEntity, Map<String, Double>> clientQuotas = adminClient.describeClientQuotas(ClientQuotaFilter.all()).entities().get();

      return clientQuotas.entrySet().stream().map(this::configuredQuota).filter(Objects::nonNull).collect(Collectors.toSet());
    } catch (InterruptedException | ExecutionException e) {
      // TODO: Improve exception handling
      throw new RuntimeException(e);
    }
  }

  public void create(Collection<ConfiguredQuota> quotas) {
    Collection<ClientQuotaAlteration> alters = quotas.stream().map(q -> {
      // For unspecified quota types, ConfigureQuota returns null. We use such null value directly in
      // ClientQuotaAlternation.Op to clear the quota types in the cluster.
      Collection<ClientQuotaAlteration.Op> ops = List.of(
          new ClientQuotaAlteration.Op(PRODUCER_BYTE_RATE, q.getProducerByteRate()),
          new ClientQuotaAlteration.Op(CONSUMER_BYTE_RATE, q.getConsumerByteRate()),
          new ClientQuotaAlteration.Op(REQUEST_PERCENTAGE, q.getRequestPercentage()));
      return new ClientQuotaAlteration(getClientQuotaEntity(q), ops);
    }).collect(Collectors.toList());

    try {
      adminClient.alterClientQuotas(alters).all().get();
    } catch (InterruptedException | ExecutionException e) {
      // TODO: Improve exception handling
      throw new RuntimeException(e);
    }
  }

  public void delete(Collection<ConfiguredQuota> quotas) {
    Collection<ClientQuotaAlteration> alters = quotas.stream().map(q ->
        new ClientQuotaAlteration(getClientQuotaEntity(q), CLEAR_ALL)).collect(Collectors.toList());

    try {
      adminClient.alterClientQuotas(alters).all().get();
    } catch (InterruptedException | ExecutionException e) {
      // TODO: Improve exception handling
      throw new RuntimeException(e);
    }
  }

  /**
   * A helper function that converts a client quota entity entry to a {@link ConfiguredQuota} and returns null if the
   * quota entry is deleted or unsupported.
   */
  private ConfiguredQuota configuredQuota(Map.Entry<ClientQuotaEntity, Map<String, Double>> quotaEntry) {
    ClientQuotaEntity entity = quotaEntry.getKey();
    if (entity.entries().keySet().stream().anyMatch(Predicate.not(SUPPORTED_ENTITY_KEYS::contains))) {
      LOG.warn("Skipping unsupported quota entity {}.", entity);
      return null;
    }

    Map<String, Double> quotas = quotaEntry.getValue();
    List<String> unsupportedQuotaTypes = quotas.keySet().stream().filter(Predicate.not(SUPPORTED_QUOTA_TYPES::contains))
        .collect(Collectors.toUnmodifiableList());
    if (!unsupportedQuotaTypes.isEmpty()) {
      LOG.warn("Ignoring unsupported quota types {} of entity {}.", unsupportedQuotaTypes, entity);
    }
    // Skips the entries that don't contain any supported quota types to skip previously deleted quota entries.
    if (Collections.disjoint(quotas.keySet(), SUPPORTED_QUOTA_TYPES)) {
      return null;
    }

    return new ConfiguredQuota(
        getEntityName(entity, ClientQuotaEntity.USER),
        getEntityName(entity, ClientQuotaEntity.CLIENT_ID),
        quotas.get(PRODUCER_BYTE_RATE),
        quotas.get(CONSUMER_BYTE_RATE),
        quotas.get(REQUEST_PERCENTAGE));
  }

  /**
   * A helper function to extract the entity name under a key from a {@link ClientQuotaEntity} while converting the
   * null-value / default-value semantics to follow {@link ConfiguredQuota}:
   * <ul>
   *   <li>Explicit default entity is expressed as a constant string "&lt;default&gt;" instead of null;</li>
   *   <li>Unspecified entity name is expressed as null instead of absence.</li>
   * </ul>
   */
  private String getEntityName(ClientQuotaEntity entity, String key) {
    if (!entity.entries().containsKey(key)) {
      return null;
    }
    if (entity.entries().get(key) == null) {
      return ConfiguredQuota.ENTITY_DEFAULT;
    }
    return entity.entries().get(key);
  }


  /**
   * A helper function to set the entity name under a key in a {@link ClientQuotaEntity} entries while converting the
   * null-value / default-value semantics from  {@link ConfiguredQuota}:
   * <ul>
   *   <li>Explicit default entity is expressed as null instead of the string "&lt;default&gt;";</li>
   *   <li>Unspecified entity name is not set instead of set as null.</li>
   * </ul>
   */
  private void putEntityName(Map<String, String> entity, String key, String name) {
    if (name == null) {
      return;
    }
    if (name.equals(ConfiguredQuota.ENTITY_DEFAULT)) {
      entity.put(key, null);
      return;
    }
    entity.put(key, name);
  }

  private ClientQuotaEntity getClientQuotaEntity(ConfiguredQuota quota) {
    Map<String, String> entity = new HashMap<>();
    putEntityName(entity, ClientQuotaEntity.USER, quota.getPrincipal());
    putEntityName(entity, ClientQuotaEntity.CLIENT_ID, quota.getClient());
    return new ClientQuotaEntity(entity);
  }
}
