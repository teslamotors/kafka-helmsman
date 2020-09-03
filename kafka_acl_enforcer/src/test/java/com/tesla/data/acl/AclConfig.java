/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import com.tesla.data.acl.mixin.Json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.kafka.common.acl.AclBinding;

import java.io.IOException;

public class AclConfig {

  private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());

  public static final String conf =
      String.join(
          "\n",
          new String[] {
              "---",
              "- entry:",
              "    principal: User:foo",
              "    host: bar",
              "    operation: READ",
              "    permission: ALLOW",
              "  resource:",
              "    type: TOPIC",
              "    name: footopic",
              "    pattern: LITERAL"
          });

  static {
    Json.addMixIns(MAPPER);
  }

  public static AclBinding[] bindingsForTest() throws IOException {
    return MAPPER.readValue(conf, AclBinding[].class);
  }

}

