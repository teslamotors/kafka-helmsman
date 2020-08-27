/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * A dummy implementation of enforcer meant for testing.
 */
public class DummyEnforcer extends Enforcer<Dummy> {

  DummyEnforcer(
      Collection<Dummy> configured, Supplier<Collection<Dummy>> existing, boolean safemode) {
    super(configured, existing, Dummy::equals, safemode);
  }

  DummyEnforcer(Collection<Dummy> configured, Supplier<Collection<Dummy>> existing) {
    this(configured, existing, true);
  }

  @Override
  protected void create(List<Dummy> toCreate) {
    // no-op
  }

  @Override
  protected void delete(List<Dummy> toDelete) {
    // no-op
  }
}
