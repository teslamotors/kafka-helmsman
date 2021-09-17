
# Kafka Consumer Freshness Tracker

The Consumer Freshness Tracker is a tool to track the "freshness" of a consumer - how far behind in time the consumer
is from the head (Log End Offset) of a queue. This is separate from Burrow to allow it to track the timestamp and then
do a delta to the LEO. Internally, it leverages both [Burrow](https://github.com/linkedin/Burrow) and direct queries
against Kafka.

Metrics are exported via Prometheus at `localhost:8081/metrics` and continuously updated, until stopped.

## Features

 * Commit Lag
   * The amount of time the most recent committed offset has spent in the queue before it is marked committed
 * Freshness
   * Difference between when the most recent committed offset first entered the queue and the timestamp of the most 
   recent added offset (the Log End Offset - LEO - in Kafka parlance).

## Dependencies

* JVM
* Burrow

## Installation

### Build an executable jar

```
bazel build //kafka_consumer_freshness_tracker/src/main/java/com/tesla/data/consumer/freshness:ConsumerFreshness_deploy.jar
```

### Verify

```
java -jar bazel-bin/kafka_consumer_freshness_tracker/src/main/java/com/tesla/data/consumer/freshness/ConsumerFreshness_deploy.jar --help
```

Copy this jar to the desired path.

```
bazel-bin/kafka_consumer_freshness_tracker/src/main/java/com/tesla/data/consumer/freshness/ConsumerFreshness_deploy.jar
```

> Note: Publishing pre-compiled binary is on the roadmap.

## Usage

Bundled as a jar you can run the Tracker with a configuration file with the `--conf <path to conf>` flag. For local 
testing, you can also just add the `--once` flag to just run the Tracker once, and then dump the metrics to the console, 
before stopping. 
The cluster configurations are validated before the Tracker is run. Validation will fail if Burrow is unreachable, if
the cluster is unknown to Burrow, or if there is an inconsistency between the bootstrap servers advertised by Burrow and
those listed in the Tracker configuration. The definition of inconsistent, as well as the validation failure behaviour,
depends on if the tracker is run in normal or strict mode.
In normal mode, the bootstrap server list in the configuration must not contain any servers which don't appear in Burrow,
and a warning will be logged if validation fails.
In strict mode, the bootstrap server lists from Burrow and in the Tracker configuration must match exactly, and the Tracker
will crash if validation fails.
The Tracker runs in normal mode by default; strict mode can be enabled by supplying the `--strict` flag.

## Configuration

Configuration is read as a YAML file.

Because the Tracker reads from Burrow and Kafka, you need to configure both. It only supports reading from a single 
cluster currently. Below is a minimal configuration:

```yaml
burrow:
  url: "http://burrow.example.com"

clusters:
  - name: logs-cluster
    kafka:
      bootstrap.servers: "l1.example.com:9092, l2.example.com:9092, l3.example.com:9092"
  - name: metrics-cluster
    kafka:
      bootstrap.servers: "m1.example.com:9092, m2.example.com:9092, m3.example.com:9092"
```

The Kafka configs are passed directly to the consumer, so you can add any standard
[Kafka consumers configs](https://kafka.apache.org/documentation/#newconsumerconfigs) as needed.

### Additional Options

 * `port`
    * Port over which to expose metrics
    * **Default: 8081**
 * `frequency_sec`
   * Number of seconds between execution intervals. Should be approximately the same as the Burrow topic scrape interval
   * **Default: 30**
 * `workerThreadCount`
    * Number of concurrent workers accessing kafka
    * **Default: 15**
 * `numConsumers`
    * Number of consumer instances to create per-Kafka cluster. Applicable under each cluster, for example:
    ```yaml
    clusters:
      - name: analytics
        numConsumers: 10
        kafka:
          ...
    ```
    * **Default: 15**
      * For smaller clusters or multiple clusters, you will want to adjust this to manage memory allocation (each
      consumer takes a non-trivial chunk of memory, which can add up when monitoring multiple clusters).

## Design

Logically, the implementation is fairly simple - scrape burrow for the current state of all the consumers. Then for each consumer, ask Kafka for the timestamp of the committed offset and the latest offset, then calculate freshness and queue time.

Our implementation is slightly more complicated because running single threaded is too slow. The KafkaConsumer API is not thread safe, thus we have a little dance so that each compute task waits for an available KafkaConsumer, does its work, and then hands it back to the Tracker for use by the next compute task.
