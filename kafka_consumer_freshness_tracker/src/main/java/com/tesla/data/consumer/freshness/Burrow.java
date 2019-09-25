/*
 * Copyright © 2018 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.consumer.freshness;

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tesla.shade.com.google.common.base.Joiner;
import tesla.shade.com.google.common.collect.Lists;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Helper class to abstract out Burrow requests.
 */
class Burrow {
  private static final Logger LOG = LoggerFactory.getLogger(Burrow.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Joiner PATHS = Joiner.on('/');

  private final OkHttpClient client;
  private final String url;
  private final String api;

  public Burrow(Map<String, Object> conf) {
    this(conf, new OkHttpClient());
  }

  Burrow(Map<String, Object> conf, OkHttpClient client) {
    this.client = client;
    this.url = (String) conf.getOrDefault("url", "http://127.0.0.1:8000");
    this.api = (String) conf.getOrDefault("api", "v3/kafka");
  }

  private HttpUrl address(String... paths) {
    List<String> parts = Lists.newArrayList(this.url, this.api);
    Collections.addAll(parts, paths);
    return HttpUrl.parse(PATHS.join(parts));
  }

  private Map<String, Object> request(String... paths) throws IOException {
    HttpUrl url = address(paths);
    LOG.debug("GET {}", url);
    Request request = new Request.Builder()
        .url(url)
        .get().build();
    Response response = client.newCall(request).execute();
    return MAPPER.readValue(response.body().byteStream(), Map.class);
  }

  public List<String> consumerGroups(String cluster) throws IOException {
    return (List<String>) this.request(cluster, "consumer").get("consumers");
  }

  public Map<String, Object> getConsumerGroupStatus(String cluster, String consumerGroup) throws IOException {
    return (Map<String, Object>) this.request(cluster, "consumer", consumerGroup, "lag").get("status");
  }

  public List<ClusterClient> getClusters() throws IOException {
    return ((List<String>)this.request().get("clusters")).stream()
        .map(ClusterClient::new)
        .collect(Collectors.toList());
  }

  public class ClusterClient {

    private final String cluster;

    public ClusterClient(String cluster) {
      this.cluster = cluster;
    }

    public List<String> consumerGroups() throws IOException {
      return Burrow.this.consumerGroups(this.cluster);
    }

    public Map<String, Object> getConsumerGroupStatus(String consumerGroup) throws IOException {
      return Burrow.this.getConsumerGroupStatus(this.cluster, consumerGroup);
    }

    public String getCluster() {
      return cluster;
    }
  }
}
