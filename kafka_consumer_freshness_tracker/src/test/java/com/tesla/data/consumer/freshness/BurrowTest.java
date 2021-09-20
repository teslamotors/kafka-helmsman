/*
 * Copyright © 2018 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import tesla.shade.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BurrowTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final Map<String, Object> CONF = new HashMap<>();

  static {
    CONF.put("cluster", "cluster");
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testConsumers() throws Exception {
    OkHttpClient client = Mockito.mock(OkHttpClient.class);
    Map<String, Object> response = new HashMap<>();
    List<String> expected = Lists.newArrayList("one", "two");
    response.put("consumers", expected);
    when(client.newCall(any())).then(respondWithJson(response));

    Burrow burrow = new Burrow(CONF, client);
    assertEquals(expected, burrow.consumerGroups("cluster"));
    verify(client).newCall(expectPath("/v3/kafka/cluster/consumer"));
  }

  @Test
  public void testStatus() throws Exception {
    Map<String, Object> response = new HashMap<>();
    response.put("error", false);
    response.put("message", "consumer status returned");

    Map<String, Object> expected = new HashMap<>();
    expected.put("cluster", "cluster");
    expected.put("group", "some.group");

    List<Map<String, Object>> partitions = new ArrayList<>();
    expected.put("partitions", partitions);
    response.put("status", expected);

    OkHttpClient client = Mockito.mock(OkHttpClient.class);
    when(client.newCall(any())).then(respondWithJson(response));

    Burrow burrow = new Burrow(CONF, client);
    assertEquals(expected, burrow.getConsumerGroupStatus("cluster", "some.group"));
    verify(client).newCall(expectPath("/v3/kafka/cluster/consumer/some.group/lag"));
  }

  @Test
  public void testClusters() throws Exception {
    OkHttpClient client = Mockito.mock(OkHttpClient.class);
    Map<String, Object> response = new HashMap<>();
    List<String> expected = Lists.newArrayList("c1", "c2");
    response.put("clusters", expected);
    when(client.newCall(any())).then(respondWithJson(response));

    Burrow burrow = new Burrow(CONF, client);
    List<Burrow.ClusterClient> clients = burrow.getClusters();
    assertEquals(2, clients.size());
    assertEquals(expected, clients.stream().map(Burrow.ClusterClient::getCluster).collect(Collectors.toList()));
    verify(client).newCall(expectPath("/v3/kafka"));
  }

  @Test
  public void testThrowExceptionOnUnsuccessfulResponse() throws IOException {
    OkHttpClient client = Mockito.mock(OkHttpClient.class);
    Map<String, Object> response = new HashMap<>();
    List<String> expected = Lists.newArrayList("c1", "c2");
    response.put("clusters", expected);
    when(client.newCall(any())).then(respondWithJson(response, 404));

    Burrow burrow = new Burrow(CONF, client);
    thrown.expect(IOException.class);
    burrow.getConsumerGroupStatus("mycluster", "mygroup");
  }

  @Test
  public void testFailGroupLookupWhenResponseIsNotAMap() throws IOException {
    OkHttpClient client = Mockito.mock(OkHttpClient.class);
    when(client.newCall(any())).then(respondWithJson("foo-bar"));

    Burrow burrow = new Burrow(CONF, client);
    thrown.expect(IOException.class);
    burrow.getConsumerGroupStatus("mycluster", "mygroup");
  }

  @Test
  public void testGetClusterBootstrapServers() throws Exception {
    OkHttpClient client = Mockito.mock(OkHttpClient.class);
    List<String> expected = Lists.newArrayList("kafka01.example.com:10251", "kafka02.example.com:10251");
    Map<String, List<String>> module = new HashMap<>();
    module.put("servers", expected);
    Map<String, Object> cluster = new HashMap<>();
    cluster.put("module",module);
    Map<String, Object> response = new HashMap<>();
    response.put("cluster", cluster);
    when(client.newCall(any())).then(respondWithJson(response));

    Burrow burrow = new Burrow(CONF, client);
    List<String> servers = burrow.getClusterBootstrapServers("cluster_name");
    assertEquals(expected, servers);
  }

  private Request expectPath(String path) {
    return argThat(new BaseMatcher<Request>() {
      @Override
      public void describeTo(Description description) {
        description.appendText("Requested URL does not match expected: " + path);
      }

      @Override
      public boolean matches(Object o) {
        Request request = (Request) o;
        return request.url().encodedPath().equals(path);
      }
    });
  }

  private Answer<Call> respondWithJson(Object o) {
    return respondWithJson(o, 200);
  }

  private Answer<Call> respondWithJson(Object o, int code) {
    return new Answer<Call>() {
      @Override
      public Call answer(InvocationOnMock invocationOnMock) throws Throwable {
        Request request = (Request) invocationOnMock.getArguments()[0];
        try {
          Call call = Mockito.mock(Call.class);
          ResponseBody body = ResponseBody.create(MediaType.parse("application/json"), MAPPER.writeValueAsString(o));
          Response response = new Response.Builder()
              .request(request)
              .body(body)
              .code(code)
              .protocol(Protocol.HTTP_1_1)
              .message("Response")
              .build();
          when(call.execute()).thenReturn(response);
          return call;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
