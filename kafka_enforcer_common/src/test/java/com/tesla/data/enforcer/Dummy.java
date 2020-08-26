/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import java.util.Map;
import java.util.Objects;

/**
 * A dummy enforceable entity.
 */
public class Dummy {
  final String name;
  final Map<String, String> config;

  public Dummy(String name, Map<String, String> config) {
    if (name == null || config == null) {
      throw new IllegalArgumentException(
          String.format("Invalid args, name=%s, config=%s", name, config));
    }
    this.name = name;
    this.config = config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Dummy dummy = (Dummy) o;
    return name.equals(dummy.name) &&
        config.equals(dummy.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, config);
  }
}
