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
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Helper class to abstract out Burrow requests.
 */
class Burrow {
  private static final Logger LOG = LoggerFactory.getLogger(Burrow.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Joiner PATHS = Joiner.on('/');
  private static final String HEALTH_CHECK = "burrow/admin";

  private final OkHttpClient client;
  private final String url;
  private final String api;

  public Burrow(Map<String, Object> conf) {
    this(conf, new OkHttpClient(), true);
  }

  public Burrow(Map<String, Object> conf, OkHttpClient client) {
    this(conf, client, false);
  }

  /**
   * Creates a burrow instance.
   *
   * @param conf          app configuration
   * @param client        an http client
   * @param doHealthCheck if true, performs a health check and throws an exception if the check fails
   */
  Burrow(Map<String, Object> conf, OkHttpClient client, boolean doHealthCheck) {
    this.client = client;
    this.url = (String) conf.getOrDefault("url", "http://127.0.0.1:8000");
    this.api = (String) conf.getOrDefault("api", "v3/kafka");
    // sanity check
    if (doHealthCheck && !isHealthy()) {
      LOG.error("Burrow server at {} is not healthy, we expect {}/{} to return GOOD", url, url, HEALTH_CHECK);
      throw new IllegalStateException("Burrow is not healthy or configured incorrectly");
    }
  }

  private boolean isHealthy() {
    Request request = new Request.Builder()
        .url(PATHS.join(url, HEALTH_CHECK))
        .get().build();
    try (Response response = client.newCall(request).execute()) {
      return response.isSuccessful() && Objects.requireNonNull(response.body()).string().equals("GOOD");
    } catch (IOException e) {
      LOG.warn("Failed to execute the health check", e);
      return false;
    }
  }

  private HttpUrl address(String... paths) {
    List<String> parts = Lists.newArrayList(this.url, this.api);
    Collections.addAll(parts, paths);
    return HttpUrl.parse(PATHS.join(parts));
  }

  private Map<String, Object> request(String... paths) throws IOException {
    Response response = null;
    HttpUrl url = address(paths);
    LOG.debug("GET {}", url);
    Request request = new Request.Builder()
        .url(url)
        .get().build();
    try (Response executed = client.newCall(request).execute()) {
      // set separately so we have a hook outside in the finally block
      response = executed;
      // burrow will build a valid map with the body for invalid responses (i.e. consumer group not found), so we
      // need to check that the response was failure.
      if (!response.isSuccessful()) {
        throw new IOException("Response was not successful: " + response);
      }
      return MAPPER.readValue(response.body().byteStream(), Map.class);
    } catch (IOException e) {
      // ensure to log the response (which includes the request) for debugging purposes, its not usually going to be
      // included in the error message
      LOG.error("Failed to complete request:" + (response == null ? request : response), e);
      throw e;
    }
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

  public Map<String, Object> getClusterDetail(String cluster) throws IOException {
	  return (Map<String, Object>) this.request(cluster).get("cluster");
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

    public Map<String, Object> getClusterDetail() throws IOException {
	return Burrow.this.getClusterDetail(this.cluster);
    }
  }
}
