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

OR,

```yaml
    kafka:
      bootstrap.servers: "broker1:9092,broker2:9092,broker3:9092"
    topicsFile: /topics.yaml
```

where `/topics.yaml` has

```yaml
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

## Multi cluster configuration
To avoid repetition across clusters, the topic enforcer supports multi cluster configuration as well. A sample is shown below,

```yaml
kafka:
    bootstrap.servers: 'foo_1:9092,foo_2:9092,foo_3:9092'
topics:
  - name: topic_A
    partitions: 30
    replicationFactor: 3
    clusters:
      foo: {}
      bar:
        replicationFactor: 2
  - name: topic_B
    partitions: 10
    replicationFactor: 3
    config:
       retention.ms: "12000000"
    clusters:
      foo: {}
      baz:
        config:
          retention.ms: "6000000"
```

To use this feature, use `cluster` flag in the enforcer. Only topics relevant to the given cluster would be enforced by the enforcer. Other rules are,

* A topic applies to a cluster if that cluster is present under topic's `clusters`
attribute. If no changes to base config are desired, specify empty dict `{}`
* All topic properties can be overridden, except the name
* Map (dictionary) properties are merged in a way that preserves base settings
 

Note: `topicsFile` is supported for multi-cluster configuration too.

# Defaults

At the top-level of the configuration (where you put `kafka` and `topics` or `topicsFile`) we also support setting
`defaults` for either all topics or per-cluster. For example:
```yaml
kafka:
    bootstrap.servers: 'foo_1:9092,foo_2:9092,foo_3:9092'

topicsFile: my-topics.yaml

defaults:
  partitions: 10
  clusters:
    foo:
      config:
        retention.ms: "6000000"
```

Would default to 10 partitions for all topics in all clusters, and 6000000ms `retention.ms` for all topics in cluster
 `foo`. The defaults take similar precedence to the topic-level cluster overrides; that is, the priority is:
 
  * cluster-topic overrides
  * topic configs
  * cluster default
  * default 

