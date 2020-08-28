/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl.mixin;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourceType;

public abstract class ResourcePatternMixin {

  @JsonCreator
  public ResourcePatternMixin(
      @JsonProperty("type") ResourceType resourceType,
      @JsonProperty("name") String name,
      @JsonProperty("pattern") PatternType patternType) {
    // for jackson
  }
}
