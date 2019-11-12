package com.tesla.data.certificates.keystore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;
import com.google.common.io.Resources;

import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;

public class KafkaClientKeystoresTest {

  private static final String PASSWORD = "password";

  @Test
  public void testKeystore() throws Exception {
    InputStream key = Resources.getResource("keystore/client.key").openStream();
    InputStream cert = Resources.getResource("keystore/client.crt").openStream();
    InputStream caChain = Resources.getResource("keystore/ca_chain").openStream();

    KafkaClientKeystores keystores = new KafkaClientKeystores(PASSWORD);
    KeyStore store = keystores.createKeystore(key, cert, caChain);
    Key storedKey = store.getKey("client", PASSWORD.toCharArray());
    assertNotNull(storedKey);
    assertEquals("RSA", storedKey.getAlgorithm());
    // should be our cert + ca chain (which we know is two certs long)
    Certificate[] chain = store.getCertificateChain("client");
    assertEquals(3, chain.length);
  }

  @Test
  public void testTruststore() throws Exception {
    KafkaClientKeystores keystores = new KafkaClientKeystores(PASSWORD);
    InputStream ca = Resources.getResource("keystore/issuing_ca").openStream();
    KeyStore store = keystores.createTruststore(ca);
    assertNotNull(store.getCertificate("caroot"));
  }
}
