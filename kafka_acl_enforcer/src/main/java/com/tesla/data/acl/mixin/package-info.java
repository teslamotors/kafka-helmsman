/*
 * Copyright (c) 2020. Tesla Motors, Inc. All rights reserved.
 */

/**
 * <h3>Mixin notes:</h3>
 *
 * <p>"Mix-in" annotations are a way to associate annotations with classes, without modifying
 * (target) classes themselves, originally intended to help support 3rd party datatypes where user can not modify
 * sources to add annotations. Native ACL data structures in kafka client libraries are not JSON/Yaml friendly out of
 * the box & mixins defined in this package do just that.
 */
package com.tesla.data.acl.mixin;
