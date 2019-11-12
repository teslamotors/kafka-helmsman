/*
 * Copyright © 2019 Tesla Motors, Inc. All rights reserved.
 */

package com.tesla.data.certificates.keystore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.Random;

public class PasswordGeneratorTest {

  private static final Long SEED = 1708523881L;
  private static final Random RANDOM = new Random(SEED);
  private static final PasswordGenerator GENERATOR = new PasswordGenerator(RANDOM);

  @Test
  public void testGeneratePasswords() {
    int len = 15;
    String pass = GENERATOR.generatePassword(len, 2, 2, 2, 2);
    assertEquals(len, pass.length());
    assertContainsAll(pass, "J", "L", ".", ":");

    // do it again, but should be different b/c of randomness in some letters + scrambling
    PasswordGenerator gen = new PasswordGenerator(new Random(SEED));
    String pass2 = gen.generatePassword(len, 2, 2, 2, 2);
    assertEquals(len, pass2.length());
    assertContainsAll(pass2, "J", "L", ".", ":");
    assertNotEquals("Generated the same password twice", pass, pass2);
  }

  private void assertContainsAll(String word, String... letters) {
    for (String letter : letters) {
      assertTrue(word.contains(letter));
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testFailIfMoreRequirementsThanLength() {
    GENERATOR.generatePassword(5, 2, 2, 1, 1);
  }

  @Test
  public void testSucceedIfRequirementsEqualLength() {
    GENERATOR.generatePassword(6, 2, 2, 1, 1);
  }
}
