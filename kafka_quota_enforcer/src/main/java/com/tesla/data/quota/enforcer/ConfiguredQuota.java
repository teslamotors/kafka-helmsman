/*
 * Copyright (c) 2021. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.quota.enforcer;

import com.fasterxml.jackson.annotation.JsonProperty;
import tesla.shade.com.google.common.base.MoreObjects;
import tesla.shade.com.google.common.base.Objects;
import tesla.shade.com.google.common.base.Preconditions;

/**
 * ConfiguredQuota represents a quota and its desired configuration.
 */
public class ConfiguredQuota {
  private final String principal;
  private final String client;
  @JsonProperty("producer-byte-rate")
  private final Integer producerByteRate;
  @JsonProperty("consumer-byte-rate")
  private final Integer consumerByteRate;
  @JsonProperty("request-percentage")
  private final Integer requestPercentage;

  public ConfiguredQuota(String principal, String client, Integer producerByteRate,
                         Integer consumerByteRate, Integer requestPercentage) {
    Preconditions.checkArgument(principal != null || client != null, "Invalid quota configuration detected. " +
        "At least one of 'principal' or 'client' must be provided.");
    this.principal = principal;
    this.client = client;
    this.producerByteRate = producerByteRate;
    this.consumerByteRate = consumerByteRate;
    this.requestPercentage = requestPercentage;
  }

  public String getPrincipal() {
    return principal;
  }

  public String getClient() {
    return client;
  }

  public Integer getProducerByteRate() {
    return producerByteRate;
  }

  public Integer getConsumerByteRate() {
    return consumerByteRate;
  }

  public Integer getRequestPercentage() {
    return requestPercentage;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("principal", principal)
        .add("client", client)
        .add("producer-byte-rate", producerByteRate)
        .add("consumer-byte-rate", consumerByteRate)
        .add("request-percentage", requestPercentage)
        .toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfiguredQuota that = (ConfiguredQuota) o;
    return Objects.equal(principal, that.principal) &&
        Objects.equal(client, that.client) &&
        Objects.equal(producerByteRate, that.producerByteRate) &&
        Objects.equal(consumerByteRate, that.consumerByteRate) &&
        Objects.equal(requestPercentage, that.requestPercentage);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(principal, client, producerByteRate, consumerByteRate, requestPercentage);
  }
}