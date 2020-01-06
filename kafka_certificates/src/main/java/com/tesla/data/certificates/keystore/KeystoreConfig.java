package com.tesla.data.certificates.keystore;

import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.beust.jcommander.Parameter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Base64;
import java.util.Optional;

public class KeystoreConfig {
  @Parameter(names = "--certificate", description = "Path to the client's signed certificate", required = true)
  String certificate;

  @Parameter(names = "--key", description = "Path to the client's private key", required = true)
  String key;

  @Parameter(names = "--issuing_ca", description = "Path to the certificate for the issuing CA. If not specified, uses " +
      "the first certificate in the CA Chain")
  String issuingCa;

  @Parameter(names = "--ca_chain", description = "Path to the  certificate chain", required = true)
  String caChain;

  @Parameter(names = "--password",
      description = "Keystore & trustore password. If not provided, one will be generated for you.", password = true)
  String password;

  @Parameter(names = "--directory", description = "Optional directory where to store the keystores.")
  String directory;

  @Parameter(names = {"-h", "--help"}, help = true, description = "Show this help")
  boolean help;

  private byte[] keystore;
  private byte[] truststore;

  public InputStream key() throws IOException {
    return new FileInputStream(key);
  }

  public InputStream certificate() throws IOException {
    return new FileInputStream(certificate);
  }

  public Optional<InputStream> issuingCa() throws IOException {
    return issuingCa == null? empty(): of(new FileInputStream(issuingCa));
  }

  public InputStream caChain() throws IOException {
    return new FileInputStream(caChain);
  }

  public void setKeystore(byte[] ks) {
    this.keystore = ks;
  }

  public void setTruststore(byte[] truststorePath) {
    this.truststore = truststorePath;
  }

  /**
   * Print out the configurations for client keystores, but encodes the values as Base64 for ease of addition to
   * external configuration files (i.e. Kubernetes).
   */
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    try {
      appendFileContent("key", key, sb);
      appendFileContent("ca", issuingCa, sb);
      appendFileContent("cert", certificate, sb);
      appendBytes("keystore", keystore, sb);
      appendString("keystore.password", password, sb);
      appendBytes("truststore", truststore, sb);
      appendString("truststore.password", password, sb);
      return sb.toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void appendFileContent(String key, String filePath, StringBuilder sb) throws IOException {
    if (filePath != null) {
      // copy the bytes into a simple byte array
      byte[] content = Files.readAllBytes(new File(filePath).toPath());
      appendBytes(key, content, sb);
    }
  }

  private void appendBytes(String key, byte[] bytes, StringBuilder sb) {
    if (bytes != null) {
      appendEncodedString(key, Base64.getEncoder().encodeToString(bytes), sb);
    }
  }

  private void appendString(String key, String value, StringBuilder sb) {
    if (value != null) {
      appendEncodedString(key, Base64.getEncoder().encodeToString(value.getBytes()), sb);
    }
  }

  private void appendEncodedString(String key, String value, StringBuilder sb) {
    if (value != null) {
      sb.append(key).append(": ").append(value).append("\n");
    }
  }
}
