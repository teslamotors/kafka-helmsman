package com.tesla.data.certificates.keystore;

import org.apache.commons.lang3.RandomStringUtils;

import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class PasswordGenerator {
  private static final int A = 65;
  private static final int Z = 90;
  private static final int LOWERCASE_A = 97;
  private static final int LOWERCASE_Z = 122;
  // a selection of special characters that should be nice to configurations
  private static final char[] SPECIAL_CHARS = new char[]{
      '!', '#', '%', '&', '+', '-', '/', '\\', '(', ')', '*', '^', '=', '>', '<', ':', ';', ',', '.'
  };

  private final Random random;

  public PasswordGenerator() {
    this(new SecureRandom());
  }

  PasswordGenerator(Random random) {
    this.random = random;
  }

  public String generatePassword(int totalLength, int minUppercase, int minLowercase, int numSpecial, int numDigits) {
    int remainingLetters = totalLength - (minUppercase + minLowercase + numSpecial + numDigits);
    if (remainingLetters < 0) {
      throw new IllegalArgumentException("More requirements than minimum letters");
    }
    String upperCaseLetters = RandomStringUtils.random(minUppercase, A, Z, true, true, null, random);
    String lowerCaseLetters = RandomStringUtils.random(minLowercase, LOWERCASE_A, LOWERCASE_Z, true, true, null,
        random);
    String specialChar = RandomStringUtils.random(numSpecial, 0, 0, false, false, SPECIAL_CHARS, random);
    String numbers = RandomStringUtils.randomNumeric(numDigits);
    String remaining = RandomStringUtils.randomAlphanumeric(remainingLetters);
    String combinedChars = upperCaseLetters.concat(lowerCaseLetters)
        .concat(numbers)
        .concat(specialChar)
        .concat(remaining);
    List<String> chars = Arrays.asList(combinedChars.split(""));
    Collections.shuffle(chars);
    return chars.stream().collect(StringBuilder::new, StringBuilder::append, StringBuilder::append).toString();
  }
}
