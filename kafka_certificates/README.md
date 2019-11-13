# Kafka Certificates

A simple tool to help make it easy to convert between X.509 certificates and Java Keystores for kafka clients.

## Installation

```bash
bazel build //kafka_certificates/...
```

## Running

### Show the help

```bash
$ bazel run //kafka_certificates/src/main/java/com/tesla/data/certificates/keystore:KafkaClientKeystores --help
Usage: <main class> [options]
  Options:
  * --ca_chain
      Path to the  certificate chain
  * --certificate
      Path to the client's signed certificate
    --directory
      Optional directory where to store the keystores.
    -h, --help
      Show this help
  * --issuing_ca
      Path to the  certificate for the issuing CA
  * --key
      Path to the client's private key
    --password
      Keystore & trustore password. If not provided, one will be generated for 
      you. 

```

where fields marked with an `*` are required 

### Standard execution

```bash
$ bazel run //kafka_certificates/src/main/java/com/tesla/data/certificates/keystore:KafkaClientKeystores \
--certificate /tmp/kafka/certs/client.crt \
--key /tmp/kafka/certs/client.key \
--issuing_ca /tmp/kafka/certs/issuing_ca \
--ca_chain /tmp/kafka/certs/ca_chain \
--directory /tmp/kafka/certs 
``` 

This will take the X.509 certificates in `/tmp/kafka/certs` and make a keystore and a truststore for a Kafka client, 
as well as generate a password. It will also base64 encode all the fields and dump them to standard out, for ease of 
adding to configurations (e.g. in Kubernetes).
