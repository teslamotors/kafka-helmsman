/*
 * Copyright (c) 2019. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class EnforcerTest {

  private static final List<Dummy> dummies =
      Arrays.asList(new Dummy("a", emptyMap()), new Dummy("b", emptyMap()));
  private DummyEnforcer enforcer;

  @Test(expected = IllegalArgumentException.class)
  public void testDuplicateConfigured() {
    new DummyEnforcer(Collections.nCopies(2, dummies.get(0)), Collections::emptyList);
  }

  @Test
  public void testAbsent() {
    enforcer = new DummyEnforcer(dummies, () -> singletonList(dummies.get(0)));
    Assert.assertEquals(singletonList(dummies.get(1)), enforcer.absent());
  }

  @Test
  public void testAbsentNoneExisting() {
    enforcer = new DummyEnforcer(dummies, Collections::emptyList);
    Assert.assertEquals(dummies, enforcer.absent());
  }

  @Test
  public void testAbsentNoneConfigured() {
    enforcer = new DummyEnforcer(emptyList(), () -> dummies);
    Assert.assertTrue(enforcer.absent().isEmpty());
  }

  @Test
  public void testNoUnexpected() {
    enforcer = new DummyEnforcer(dummies, () -> dummies);
    Assert.assertTrue(enforcer.unexpected().isEmpty());
  }

  @Test
  public void testUnexpected() {
    Dummy unexpected = new Dummy("x", emptyMap());
    enforcer = new DummyEnforcer(dummies, () -> singletonList(unexpected));
    Assert.assertEquals(1, enforcer.unexpected().size());
    Assert.assertEquals(unexpected, enforcer.unexpected().get(0));
  }

  @Test(expected = IllegalStateException.class)
  public void testSafeMode() {
    enforcer = new DummyEnforcer(dummies, Collections::emptyList);
    enforcer.deleteUnexpected();
  }

  @Test
  public void testSafeModeEnforceAll() {
    Dummy unexpected = new Dummy("x", emptyMap());

    // safemode on
    enforcer = spy(new DummyEnforcer(dummies, () -> singletonList(unexpected), true));
    enforcer.enforceAll();
    verify(enforcer, times(0)).delete(anyListOf(Dummy.class));

    // safemode off
    enforcer = spy(new DummyEnforcer(dummies, () -> singletonList(unexpected), false));
    enforcer.enforceAll();
    verify(enforcer, times(1)).delete(singletonList(unexpected));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testEmptyConfigSafeModeOff() {
    enforcer = new DummyEnforcer(emptyList(), Collections::emptyList, false);
  }

  @Test(expected = IllegalStateException.class)
  public void testTooManyBeingDeleted() {
    enforcer =
        new DummyEnforcer(
            dummies,
            () -> Arrays.asList(new Dummy("x", emptyMap()), new Dummy("y", emptyMap())),
            false);
    // should not be allowed as we are deleting lot of dummies!
    enforcer.deleteUnexpected();
  }
}
