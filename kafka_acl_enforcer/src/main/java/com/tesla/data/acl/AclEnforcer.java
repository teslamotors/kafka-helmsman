/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.acl;

import com.tesla.data.enforcer.Enforcer;
import com.tesla.data.enforcer.UnmanagedPrefixes;

import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.resource.ResourcePattern;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AclEnforcer extends Enforcer<AclBinding> {
  private final AclService aclService;
  private final UnmanagedPrefixes unmanaged;
  private final boolean dryRun;

  public AclEnforcer(Collection<AclBinding> configured, AclService aclService, boolean safemode, boolean dryRun) {
    this(configured, aclService, UnmanagedPrefixes.NONE, safemode, dryRun);
  }

  /**
   * The constructor.
   *
   * @param configured desired bindings, these are created even if they are on unmanaged resources
   * @param aclService an acl service
   * @param unmanaged  prefixes of TOPIC and GROUP resources owned by an external system, existing
   *                   un-configured bindings on such resources are never deleted
   * @param safemode   if true, risky operations (ex: deletion) are skipped
   * @param dryRun     if true, no changes are made to the cluster
   */
  public AclEnforcer(Collection<AclBinding> configured, AclService aclService, UnmanagedPrefixes unmanaged,
                     boolean safemode, boolean dryRun) {
    super(configured, aclService::listExisting, AclBinding::equals, safemode);
    this.aclService = aclService;
    this.unmanaged = unmanaged;
    this.dryRun = dryRun;
    LOG.info("Unmanaged topic prefixes: {}, group prefixes: {}", unmanaged.topicPrefixes(), unmanaged.groupPrefixes());
  }

  /**
   * Get a list of bindings that are not expected to be present, excluding bindings on unmanaged resources.
   *
   * @return a list of bindings
   */
  @Override
  public List<AclBinding> unexpected() {
    Map<Boolean, List<AclBinding>> byUnmanaged =
        super.unexpected().stream().collect(Collectors.partitioningBy(this::isUnmanaged));
    List<AclBinding> skipped = byUnmanaged.get(true);
    if (!skipped.isEmpty()) {
      LOG.info("Skipping {} un-configured bindings on unmanaged resources.", skipped.size());
      LOG.debug("Skipped bindings: {}", skipped);
    }
    return Collections.unmodifiableList(byUnmanaged.get(false));
  }

  // Bindings on the wildcard resource grant access beyond any prefix, so they are always managed.
  private boolean isUnmanaged(AclBinding binding) {
    ResourcePattern pattern = binding.pattern();
    if (ResourcePattern.WILDCARD_RESOURCE.equals(pattern.name())) {
      return false;
    }
    switch (pattern.resourceType()) {
      case TOPIC:
        return unmanaged.isUnmanagedTopic(pattern.name());
      case GROUP:
        return unmanaged.isUnmanagedGroup(pattern.name());
      default:
        return false;
    }
  }

  @Override
  protected void create(List<AclBinding> toCreate) {
    if (!dryRun) {
      aclService.create(toCreate);
    }
  }

  @Override
  protected void delete(List<AclBinding> toDelete) {
    if (!dryRun) {
      aclService.delete(toDelete);
    }
  }

}
