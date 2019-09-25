# Kakfa Topic Enforcer

Kafka topic enforcer's goal is to automate Kafka topic management & hence remove the toil associated with doing it manually.

## Features

* Enables your Kafka topic configurations to be under version control
* Applies uniform central control to topics and enforces best practices consistently (aka idiot proofing) 
* Self service, removed dependency on a human
* Simple configuration

## Dependencies

* JVM 

## Installation

### Build an executable jar

```
bazel build //kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer:Main_deploy.jar
```

### Verify

```
java -jar bazel-bin/kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer/Main_deploy.jar --help
```

Copy this jar to the desired path.

```
bazel-bin/kafka_topic_enforcer/src/main/java/com/tesla/data/topic/enforcer/Main_deploy.jar
```

> Note: Publishing pre-compiled binary is on the roadmap.

## Usage

`java -jar Main_deploy.jar`

```bash
Usage: <main class> [options] [command] [command options]
  Options:
    --help, -h

  Commands:
    dump      Dump existing cluster config on stdout
      Usage: dump path to a configuration file

    enforce      Enforce given configuration
      Usage: enforce [options] path to a configuration file
        Options:
          --continuous, -c
            run enforcement continuously
            Default: false
          --dryrun, -d
            do a dry run
            Default: false
          --interval, -i
            run interval for continuous mode in seconds
            Default: 600
          --unsafemode
            run in unsafe mode, topic deletion is _only_ allowed in this mode
            Default: false
```

## Configuration

Sample configuration

```yaml
    kafka:
      bootstrap.servers: "broker1:9092,broker2:9092,broker3:9092"
    topics:
      - name: "topic_A"
        partitions: 30
        replicationFactor: 3

      - name: "topic_B"
        partitions: 32
        replicationFactor: 2
        config:
          cleanup.policy: "delete"
          retention.ms: "12000000"
```
