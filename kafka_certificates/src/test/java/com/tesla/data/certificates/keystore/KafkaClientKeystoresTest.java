package com.tesla.data.certificates.keystore;

import static com.tesla.data.certificates.keystore.KafkaClientKeystores.certFactory;
import static com.tesla.data.certificates.keystore.KafkaClientKeystores.readCertificateChain;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.io.Resources;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Optional;

public class KafkaClientKeystoresTest {

  private static final String PASSWORD = "password";
  private static final String ISSUER_CN = "C=US,O=Tesla\\, Inc.,OU=PKI,CN=Corporate NA Device Issuing CA 01";
  private static final String ROOT_CN = "C=US,O=Tesla\\, Inc.,OU=PKI,CN=Corporate Root CA";

  @Test
  public void testKeystore() throws Exception {
    InputStream key = stream("client.key");
    InputStream cert = stream("client.crt");
    InputStream caChain = stream("ca_chain");

    KafkaClientKeystores keystores = new KafkaClientKeystores(PASSWORD);
    KeyStore store = keystores.createKeystore(key, cert, caChain);
    verifyKeystore(store);
  }


  @Test
  public void testKeystoreWithRSAKey() throws Exception {
    InputStream key = stream("client-rsa.key");
    InputStream cert = stream("client.crt");
    InputStream caChain = stream("ca_chain");

    KafkaClientKeystores keystores = new KafkaClientKeystores(PASSWORD);
    KeyStore store = keystores.createKeystore(key, cert, caChain);
    verifyKeystore(store);
  }

  private void verifyKeystore(KeyStore store) throws Exception{
    Key storedKey = store.getKey("client", PASSWORD.toCharArray());
    assertNotNull(storedKey);
    assertEquals("RSA", storedKey.getAlgorithm());
    // should be our cert + ca chain (which we know is two certs long)
    Certificate[] chain = store.getCertificateChain("client");
    assertEquals(3, chain.length);
    assertCertificateSubjectName(chain[1], ISSUER_CN);
    assertCertificateSubjectName(chain[2], ROOT_CN);
  }

  @Test
  public void testTruststore() throws Exception {
    KafkaClientKeystores keystores = new KafkaClientKeystores(PASSWORD);
    InputStream ca = stream("issuing_ca");
    KeyStore store = keystores.createTruststore(readCertificateChain(certFactory, ca).get(0));
    assertCertificateSubjectName(store.getCertificate("caroot"), ISSUER_CN);
  }

  @Test
  public void testTruststoreFromConfig() throws Exception {
    KafkaClientKeystores keystores = new KafkaClientKeystores(PASSWORD);
    KeyStore store = keystores.createTruststore(new ResourceStreamConfig());
    assertCertificateSubjectName(store.getCertificate("caroot"), ISSUER_CN);
  }

  /**
   * When there is no issuing CA, it should use the certificate in the certificate chain
   * @throws Exception
   */
  @Test
  public void testTruststoreFromConfigWithEmptyIssuingCa() throws Exception {
    KafkaClientKeystores keystores = new KafkaClientKeystores(PASSWORD);
    KeyStore store = keystores.createTruststore(new ResourceStreamConfig(){
      @Override
      public Optional<InputStream> issuingCa() {
        return empty();
      }
    });
    assertCertificateSubjectName(store.getCertificate("caroot"), ISSUER_CN);
  }

  private void assertCertificateSubjectName(Certificate cert, String name){
    assertEquals(name, ((X509Certificate)cert).getSubjectDN().getName());
  }

  private static class ResourceStreamConfig extends KeystoreConfig {

    @Override
    public InputStream key() throws IOException {
      return stream("client.key");
    }

    @Override
    public InputStream certificate() throws IOException {
      return stream("client.cert");
    }

    @Override
    public Optional<InputStream> issuingCa() throws IOException {
      return of(stream("issuing_ca"));
    }

    @Override
    public InputStream caChain() throws IOException {
      return stream("ca_chain");
    }
  }

  private static InputStream stream(String file) throws IOException {
    return Resources.getResource("keystore/"+file).openStream();
  }
}
