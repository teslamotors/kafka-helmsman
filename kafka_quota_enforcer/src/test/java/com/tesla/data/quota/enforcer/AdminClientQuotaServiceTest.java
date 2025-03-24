/*
 * Copyright (c) 2025. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterClientQuotasResult;
import org.apache.kafka.clients.admin.DescribeClientQuotasResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AdminClientQuotaServiceTest {

  @Mock
  private AdminClient adminClient;

  @Captor
  private ArgumentCaptor<Collection<ClientQuotaAlteration>> captor;

  @Before
  public void init() {
    MockitoAnnotations.openMocks(this);
  }


  /**
   * A helper function to allow us to initialize a {@link Map} with null values.
   * The static factory does not allow null values, but Kafka API is using null values for specific purposes.
   */
  private static <K, V> Map<K, V> nullableMap(Collection<Map.Entry<K, V>> entries) {
    Map<K, V> m = new HashMap<>();
    entries.forEach(e -> m.put(e.getKey(), e.getValue()));
    return m;
  }

  @Test
  public void testListExisting() {
    Map<ClientQuotaEntity, Map<String, Double>> existingQuotas = Map.of(
        // explicit user and explicit client
        new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user1", ClientQuotaEntity.CLIENT_ID, "client1")),
        Map.of("producer_byte_rate", 100d),
        // explicit user and unspecified client
        new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user1")),
        Map.of("producer_byte_rate", 3000d, "consumer_byte_rate", 2000d),
        // explicit client and unspecified user with unsupported quota props
        new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "client1")),
        Map.of("producer_byte_rate", 150d, "request_percentage", 200d, "unsupported-quota", 1d),
        // default user and default client as expressed by the Kafka API convention
        new ClientQuotaEntity(nullableMap(List.of(
            new AbstractMap.SimpleEntry<>(ClientQuotaEntity.USER, null),
            new AbstractMap.SimpleEntry<>(ClientQuotaEntity.CLIENT_ID, null)
        ))),
        Map.of("producer_byte_rate", 200d, "consumer_byte_rate", 500d, "request_percentage", 80d),
        // default user and unspecified client as expressed by the Kafka API convention
        new ClientQuotaEntity(nullableMap(List.of(
            new AbstractMap.SimpleEntry<>(ClientQuotaEntity.USER, null)
        ))),
        Map.of("producer_byte_rate", 7000d, "consumer_byte_rate", 6000d),
        // default client and unspecified user as expressed by the Kafka API convention
        new ClientQuotaEntity(nullableMap(List.of(
            new AbstractMap.SimpleEntry<>(ClientQuotaEntity.CLIENT_ID, null)
        ))),
        Map.of("producer_byte_rate", 5000d),
        // entry with empty props should be ignored (signifies a deleted config)
        new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "deleted", ClientQuotaEntity.CLIENT_ID, "deleted")),
        Map.of(),
        // entry with only unsupported props should be ignored (signifies a deleted config)
        new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "ignored")), Map.of("unsupported-quota", 1d),
        // entry with unsupported entity types are ignored
        new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user1", "OCCUPATION", "pilot")),
        Map.of("producer_byte_rate", 3000d, "consumer_byte_rate", 2000d),
        // fractional byte rates are truncated to integers
        new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "fractional")),
        Map.of("producer_byte_rate", 3000.1, "consumer_byte_rate", 2000.2, "request_percentage", 80.1)
    );

    Set<ConfiguredQuota> expected = Set.of(
        new ConfiguredQuota("user1", "client1", 100, null, null),
        new ConfiguredQuota("user1", null, 3000, 2000, null),
        new ConfiguredQuota(null, "client1", 150, null, 200d),
        new ConfiguredQuota("<default>", "<default>", 200, 500, 80d),
        new ConfiguredQuota("<default>", null, 7000, 6000, null),
        new ConfiguredQuota(null, "<default>", 5000, null, null),
        new ConfiguredQuota("fractional", null, 3000, 2000, 80.1)
    );

    when(adminClient.describeClientQuotas(ClientQuotaFilter.all()))
        .thenReturn(new DescribeClientQuotasResult(KafkaFuture.completedFuture(existingQuotas)));
    AdminClientQuotaService svc = new AdminClientQuotaService(adminClient);
    Set<ConfiguredQuota> actual = new HashSet<>(svc.listExisting());

    Assert.assertEquals(expected, actual);
  }

  // We have to manually asset because Kafka library does not override equals function of ClientQuotaAlteration.
  private void assertClientQuotaAlterationCollectionEquals(
      Collection<ClientQuotaAlteration> expected, Collection<ClientQuotaAlteration> actual) {
    Assert.assertEquals("size diffs", expected.size(), actual.size());

    Iterator<ClientQuotaAlteration> ei = expected.iterator();
    Iterator<ClientQuotaAlteration> ai = actual.iterator();
    while (ei.hasNext() && ai.hasNext()) {
      ClientQuotaAlteration e = ei.next();
      ClientQuotaAlteration a = ai.next();

      Assert.assertEquals(e.entity(), a.entity());
      Assert.assertEquals("check ops for " + e.entity().toString(), e.ops(), a.ops());
    }
  }

  @Test
  public void testCreateQuotas() {
    List<ConfiguredQuota> toCreate = List.of(
        new ConfiguredQuota("user1", "client1", 101, null, null),
        new ConfiguredQuota("user1", "<default>", 100, null, null),
        new ConfiguredQuota("user2", null, null, null, 40d),
        new ConfiguredQuota("<default>", null, 7000, 6000, null),
        new ConfiguredQuota(null, "client1", 150, null, 200d),
        new ConfiguredQuota(null, "<default>", 5000, null, null)
    );

    Collection<ClientQuotaAlteration> expected = List.of(
        new ClientQuotaAlteration(
            new ClientQuotaEntity(Map.of(
                ClientQuotaEntity.USER, "user1", ClientQuotaEntity.CLIENT_ID, "client1")),
            List.of(
                new ClientQuotaAlteration.Op("producer_byte_rate", 101d),
                new ClientQuotaAlteration.Op("consumer_byte_rate", null),
                new ClientQuotaAlteration.Op("request_percentage", null)
            )
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(nullableMap(List.of(
                new AbstractMap.SimpleEntry<>(ClientQuotaEntity.USER, "user1"),
                new AbstractMap.SimpleEntry<>(ClientQuotaEntity.CLIENT_ID, null)
            ))),
            List.of(
                new ClientQuotaAlteration.Op("producer_byte_rate", 100d),
                new ClientQuotaAlteration.Op("consumer_byte_rate", null),
                new ClientQuotaAlteration.Op("request_percentage", null)
            )
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user2")),
            List.of(
                new ClientQuotaAlteration.Op("producer_byte_rate", null),
                new ClientQuotaAlteration.Op("consumer_byte_rate", null),
                new ClientQuotaAlteration.Op("request_percentage", 40d)
            )
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(nullableMap(List.of(new AbstractMap.SimpleEntry<>(ClientQuotaEntity.USER, null)))),
            List.of(
                new ClientQuotaAlteration.Op("producer_byte_rate", 7000d),
                new ClientQuotaAlteration.Op("consumer_byte_rate", 6000d),
                new ClientQuotaAlteration.Op("request_percentage", null)
            )
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "client1")),
            List.of(
                new ClientQuotaAlteration.Op("producer_byte_rate", 150d),
                new ClientQuotaAlteration.Op("consumer_byte_rate", null),
                new ClientQuotaAlteration.Op("request_percentage", 200d)
            )
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(nullableMap(List.of(
                new AbstractMap.SimpleEntry<>(ClientQuotaEntity.CLIENT_ID, null)))),
            List.of(
                new ClientQuotaAlteration.Op("producer_byte_rate", 5000d),
                new ClientQuotaAlteration.Op("consumer_byte_rate", null),
                new ClientQuotaAlteration.Op("request_percentage", null)
            )
        )
    );

    when(adminClient.alterClientQuotas(anyCollection())).thenReturn(new AlterClientQuotasResult(
        Map.of(new ClientQuotaEntity(Map.of("don't", "matter")), KafkaFuture.completedFuture(null))));
    AdminClientQuotaService svc = new AdminClientQuotaService(adminClient);
    svc.create(toCreate);
    verify(adminClient).alterClientQuotas(captor.capture());
    Collection<ClientQuotaAlteration> actual = captor.getValue();

    assertClientQuotaAlterationCollectionEquals(expected, actual);
  }

  @Test
  public void testDeleteQuotas() {
    List<ConfiguredQuota> toDelete = List.of(
        new ConfiguredQuota("user1", "client1", 101, null, null),
        new ConfiguredQuota("user1", "<default>", 100, null, null),
        new ConfiguredQuota("user2", null, null, null, 40d),
        new ConfiguredQuota("<default>", null, 7000, 6000, null),
        new ConfiguredQuota(null, "client1", 150, null, 200d),
        new ConfiguredQuota(null, "<default>", 5000, null, null)
    );

    final Collection<ClientQuotaAlteration.Op> CLEAR_ALL = List.of(
        new ClientQuotaAlteration.Op("producer_byte_rate", null),
        new ClientQuotaAlteration.Op("consumer_byte_rate", null),
        new ClientQuotaAlteration.Op("request_percentage", null)
    );

    Collection<ClientQuotaAlteration> expected = List.of(
        new ClientQuotaAlteration(
            new ClientQuotaEntity(Map.of(
                ClientQuotaEntity.USER, "user1", ClientQuotaEntity.CLIENT_ID, "client1")),
            CLEAR_ALL
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(nullableMap(List.of(
                new AbstractMap.SimpleEntry<>(ClientQuotaEntity.USER, "user1"),
                new AbstractMap.SimpleEntry<>(ClientQuotaEntity.CLIENT_ID, null)
            ))),
            CLEAR_ALL
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.USER, "user2")),
            CLEAR_ALL
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(nullableMap(List.of(new AbstractMap.SimpleEntry<>(ClientQuotaEntity.USER, null)))),
            CLEAR_ALL
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(Map.of(ClientQuotaEntity.CLIENT_ID, "client1")),
            CLEAR_ALL
        ),
        new ClientQuotaAlteration(
            new ClientQuotaEntity(nullableMap(List.of(
                new AbstractMap.SimpleEntry<>(ClientQuotaEntity.CLIENT_ID, null)))),
            CLEAR_ALL
        )
    );

    when(adminClient.alterClientQuotas(anyCollection())).thenReturn(new AlterClientQuotasResult(
        Map.of(new ClientQuotaEntity(Map.of("don't", "matter")), KafkaFuture.completedFuture(null))));
    AdminClientQuotaService svc = new AdminClientQuotaService(adminClient);
    svc.delete(toDelete);
    verify(adminClient).alterClientQuotas(captor.capture());
    Collection<ClientQuotaAlteration> actual = captor.getValue();

    assertClientQuotaAlterationCollectionEquals(expected, actual);
  }
}
