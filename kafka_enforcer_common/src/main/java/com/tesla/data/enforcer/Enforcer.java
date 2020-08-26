/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.enforcer;

import static tesla.shade.com.google.common.base.Preconditions.checkArgument;
import static tesla.shade.com.google.common.base.Preconditions.checkState;

import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Enforcer detects a drift between expected & actual cluster state, if drift is found it enforces the desired state.
 *
 * @param <T> type of the enforceable entity.
 */
public abstract class Enforcer<T> {

  protected static final Logger LOG = LoggerFactory.getLogger(Enforcer.class);
  // we do not allow more than this % of entities to be deleted in a single run
  protected static final float PERMISSIBLE_DELETION_THRESHOLD = 0.50f;
  protected static final Gauge absentEntities = Gauge.build()
      .name("kafka_enforcer_absent")
      .help("Count of entities that are absent from the cluster.")
      .register();
  protected static final Gauge unexpectedEntities = Gauge.build()
      .name("kafka_enforcer_unexpected")
      .help("Count of entities that are unexpectedly present in the cluster.")
      .register();
  protected static final Gauge configuredEntities = Gauge.build()
      .name("kafka_enforcer_configured")
      .help("Count of configured entities.")
      .register();

  protected final Collection<T> configured;
  protected final Supplier<Collection<T>> existing;
  protected final BiPredicate<T, T> isEqual;
  protected final boolean safemode;

  /**
   * The constructor.
   *
   * @param configured list of configured entities, ex: topics, acls, etc., this list defines the desired state
   * @param existing   a supplier to fetch the actual state
   * @param isEqual    a predicate to check if two entities are same
   * @param safemode   if true, certain risky operations (ex: deletion) are skipped
   */
  protected Enforcer(Collection<T> configured, Supplier<Collection<T>> existing, BiPredicate<T, T> isEqual, boolean safemode) {
    long distinct = configured.stream().distinct().count();
    checkArgument(configured.size() == distinct, "Found duplicate entities in config");
    checkArgument(safemode || configured.size() != 0, "Configured entities should not be empty if safemode is off");
    this.isEqual = isEqual;
    this.configured = configured;
    this.existing = existing;
    this.safemode = safemode;
  }

  /**
   * Alter entities whose configuration as drifted. Default behavior is a no-op.
   */
  protected void alterDrifted() {
    // no-op
  }

  /**
   * Create new entities.
   *
   * @param toCreate a list of entities that need to be created
   */
  protected abstract void create(List<T> toCreate);

  /**
   * Delete existing entities.
   *
   * @param toDelete a list of entities that need to be deleted
   */
  protected abstract void delete(List<T> toDelete);

  /**
   * Get a list of entities that are expected to be present, but they are not.
   *
   * @return a list of entities
   */
  public List<T> absent() {
    if (this.configured.isEmpty()) {
      return Collections.emptyList();
    }
    Collection<T> existing = this.existing.get();
    return Collections.unmodifiableList(
        this.configured
            .stream()
            .filter(c -> existing.stream().noneMatch(e -> isEqual.test(c, e)))
            .collect(Collectors.toList())
    );
  }

  /**
   * Get a list of entities that are not expected to be present, but they are.
   *
   * @return a list of entities
   */
  public List<T> unexpected() {
    return Collections.unmodifiableList(
        existing.get()
            .stream()
            .filter(e -> this.configured.stream().noneMatch(c -> isEqual.test(c, e)))
            .collect(Collectors.toList()));
  }

  /**
   * Create missing entities.
   */
  public void createAbsent() {
    List<T> toCreate = absent();
    if (!toCreate.isEmpty()) {
      LOG.info("Creating {} new entities: {}", toCreate.size(), toCreate);
      create(toCreate);
    } else {
      LOG.info("No new entity was discovered.");
    }
  }

  /**
   * Delete un-expected entities.
   */
  public void deleteUnexpected() {
    checkState(!safemode, "Deletion is not allowed in safe mode");
    checkState(configured.size() > 0, "Configured entities can not be empty");
    List<T> toDelete = unexpected();
    float destruction = toDelete.size() * 1.0f / configured.size();
    checkState(destruction <= PERMISSIBLE_DELETION_THRESHOLD,
        "Too many [%s%] entities being deleted in one run", destruction * 100);
    if (!toDelete.isEmpty()) {
      LOG.info("Deleting {} redundant entities: {}", toDelete.size(), toDelete);
      delete(toDelete);
    } else {
      LOG.info("No un-expected entity was found.");
    }
  }

  /**
   * Run enforcement.
   */
  public void enforceAll() {
    LOG.info("\n--Enforcement run started--");
    createAbsent();
    alterDrifted();
    if (!safemode) {
      deleteUnexpected();
    }
    LOG.info("\n--Enforcement run ended--");
  }

  /**
   * Record stats.
   */
  public void stats() {
    absentEntities.set(absent().size());
    unexpectedEntities.set(unexpected().size());
    configuredEntities.set(configured.size());
  }

}
