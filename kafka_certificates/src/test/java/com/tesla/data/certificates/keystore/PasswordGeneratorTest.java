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
    assertMinCounts(pass, 2, 2, 2, 2);

    // do it again with the same generator, should be different b/c of randomness
    String pass2 = GENERATOR.generatePassword(len, 2, 2, 2, 2);
    assertEquals(len, pass2.length());
    assertMinCounts(pass2, 2, 2, 2, 2);
    assertNotEquals("Generated the same password twice", pass, pass2);
  }

  @Test
  public void testInjectedRandomIsOnlySourceOfRandomness() {
    // two generators seeded identically must generate identical passwords; anything
    // else means part of the password came from an RNG other than the injected one
    String pass = new PasswordGenerator(new Random(SEED)).generatePassword(15, 2, 2, 2, 2);
    String pass2 = new PasswordGenerator(new Random(SEED)).generatePassword(15, 2, 2, 2, 2);
    assertEquals("Same seed should generate the same password", pass, pass2);
  }

  private void assertMinCounts(String word, int minUppercase, int minLowercase, int minSpecial, int minDigits) {
    assertTrue(word.chars().filter(Character::isUpperCase).count() >= minUppercase);
    assertTrue(word.chars().filter(Character::isLowerCase).count() >= minLowercase);
    assertTrue(word.chars().filter(c -> !Character.isLetterOrDigit(c)).count() >= minSpecial);
    assertTrue(word.chars().filter(Character::isDigit).count() >= minDigits);
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
