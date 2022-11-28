package com.tesla.data.certificates.keystore;

import com.beust.jcommander.JCommander;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Utility to generate the client keystores and truststores from X.509 certificates.
 */
public class KafkaClientKeystores {

  private static final int PASSWORD_LENGTH = 32;
  private static final String BOUNCY_CASTLE_TYPE = "BC";
  private static final String JAVA_KEYSTORE = "jks";
  private static final PasswordGenerator passwordGenerator = new PasswordGenerator();
  private static final X509Certificate[] EMPTY_CERTS = new X509Certificate[0];

  private static final String CLIENT_KEY_NAME = "client";
  private static final String CA_ROOT_CERT_NAME = "caroot";

  // bouncycastle is not thread safe, but that's ok since this is just accessed from command line
  static final CertificateFactory certFactory;

  static {
    Security.addProvider(new BouncyCastleProvider());

    try {
      certFactory = CertificateFactory.getInstance("X.509");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private final char[] password;

  public KafkaClientKeystores(String password) {
    this.password = password.toCharArray();
  }

  /**
   * Create the keystore from the configuration.
   */
  public KeyStore createKeystore(KeystoreConfig conf) throws IOException, CertificateException,
      NoSuchAlgorithmException, KeyStoreException {
    return this.createKeystore(conf.key(), conf.certificate(), conf.caChain());
  }

  /**
   * Create a keystore that serves the private key under the alias "client", where the key has the given certificate
   * and associated Certificate Authority (CA) chain.
   *
   * @param privateKey private key for the client.
   * @param certificate certificate verifying the private key, provided by the CA.
   * @param caChain chain of certificates for the CA back to the root
   * @return a keystore for the private key + chain of certificates
   */
  public KeyStore createKeystore(InputStream privateKey, InputStream certificate,
                                 InputStream caChain) throws KeyStoreException, CertificateException, NoSuchAlgorithmException, IOException {
    // initialize the keystore
    KeyStore ks = KeyStore.getInstance(JAVA_KEYSTORE);
    // need to load to initialize the keystore for use
    ks.load(null, password);

    // read the private key
    PEMParser parser = new PEMParser(new InputStreamReader(privateKey));
    Object key = parser.readObject();
    if (key instanceof PEMKeyPair) {
      key = ((PEMKeyPair) key).getPrivateKeyInfo();
    }
    // either it was a key pair, in which case we got the private key, or it already was an unencrypted PEM private
    // key, so we can use it directly. We don't understand anything else.
    if (!(key instanceof PrivateKeyInfo)) {
      throw new IllegalArgumentException("Expected an RSA/DSA/ECDSA or an unencrypted PEM type key, but got a " + key);
    }

    JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BOUNCY_CASTLE_TYPE);
    PrivateKey pk = converter.getPrivateKey((PrivateKeyInfo) key);

    // build the certificate chain for the key
    List<X509Certificate> chain = readCertificateChain(certFactory, certificate);
    chain.addAll(readCertificateChain(certFactory, caChain));

    ks.setKeyEntry(CLIENT_KEY_NAME, pk, password, chain.toArray(EMPTY_CERTS));
    return ks;
  }

  /**
   * Create a truststore from the {@link KeystoreConfig}. If the {@link KeystoreConfig#issuingCa} is not present, uses
   * the first certificate (the end of the chain, i.e. the CA with the lowest authority) as the Issuing CA. Any
   * certificates that are signed by the issuing CA are considered trusted and will be authenticated correctly.
   *
   * @return a Keystore that trusts (i.e. a truststore) any certificate signed by the configured certificate authority
  */
  public KeyStore createTruststore(KeystoreConfig conf) throws IOException, CertificateException, KeyStoreException,
      NoSuchAlgorithmException {
    InputStream is = conf.issuingCa().orElse(conf.caChain());
    List<X509Certificate> chain = readCertificateChain(certFactory, is);
    return createTruststore(chain.get(0));
  }

  /**
   * Create keystore for the certificates that the client client should trust. Any certificates that are signed by
   * the issuing CA (whose certificate is added here) are considered trusted entities and will be authenticated
   * correctly.
   *
   * @param certificate certificate for the issuing CA whose signed certificates should be trusted
   */
  public KeyStore createTruststore(X509Certificate certificate) throws KeyStoreException, CertificateException,
      IOException, NoSuchAlgorithmException {
    KeyStore truststore = KeyStore.getInstance(JAVA_KEYSTORE);
    truststore.load(null, password);

    truststore.setCertificateEntry(CA_ROOT_CERT_NAME, certificate);
    return truststore;
  }

  public static List<X509Certificate> readCertificateChain(CertificateFactory factory, InputStream is)
      throws IOException, CertificateException {
    List<X509Certificate> certs = new ArrayList<>();
    while (is.available() > 0) {
      Object o = factory.generateCertificate(is);
      certs.add((X509Certificate) o);
    }
    return certs;
  }

  /**
   * Write the keystore to the given directory. If the specified directory is null-ed, does not attempt to write the
   * file.
   *
   * @param directory directory in which to store the keystore
   * @param type e.g. "keystore" or "truststore"
   * @param store keystore to write
   * @return the bytes of the keystore
   */
  public byte[] writeStore(Optional<String> directory, String type, KeyStore store) throws IOException,
      CertificateException,
      NoSuchAlgorithmException, KeyStoreException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
      store.store(bos, password);
      bos.flush();

      byte[] written = bos.toByteArray();
      if (directory.isPresent()) {
        String path = String.format("%s/client.%s.jks", directory.get(), type);
        try (OutputStream os = new FileOutputStream(path)) {
          os.write(written);
        }
        System.out.println("Wrote " + type + " to " + path);
      }
      return written;
    }
  }

  public static void main(String[] argv) throws Exception {
    KeystoreConfig conf = new KeystoreConfig();
    JCommander jc = JCommander.newBuilder().addObject(conf).build();
    jc.parse(argv);
    if (conf.help) {
      jc.usage();
      return;
    }
    String password = conf.password;
    if (password == null || password.length() == 0) {
      password = passwordGenerator.generatePassword(PASSWORD_LENGTH, 2, 2, 2, 2);
    }
    conf.password = password;

    KafkaClientKeystores keystores = new KafkaClientKeystores(password);

    Optional<String> directory = Optional.ofNullable(conf.directory);
    KeyStore ks = keystores.createKeystore(conf);
    byte[] keystore = keystores.writeStore(directory, "keystore", ks);
    conf.setKeystore(keystore);

    KeyStore truststore = keystores.createTruststore(conf);
    keystore = keystores.writeStore(directory, "truststore", truststore);
    conf.setTruststore(keystore);

    System.out.println();
    System.out.println("Variables for configurations:");
    System.out.println(conf);
  }
}
