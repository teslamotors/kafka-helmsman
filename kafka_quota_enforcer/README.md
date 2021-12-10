# Kakfa Quota Enforcer

Kafka quota enforcer's goal is to automate Kafka quota management, in order to mitigate denial-of-service risks and 
monopolization of broker resources from bad-acting clients.

## Features

* Enables your Kafka quota configurations to be under version control
* Applies uniform central control to quotas
* Self service, removed dependency on a human
* Simple configuration

## Dependencies

* JVM 

## Installation

### Build an executable jar

```
bazel build //kafka_quota_enforcer/src/main/java/com/tesla/data/quota/enforcer:Main_deploy.jar
```
- This will provide a path to the built executable

### Verify

```
java -jar bazel-bin/kafka_quota_enforcer/src/main/java/com/tesla/data/quota/enforcer/Main_deploy.jar --help
```
- Make sure this prints out the command usage information.

Copy this jar to the desired path.

```
bazel-bin/kafka_quota_enforcer/src/main/java/com/tesla/data/quota/enforcer/Main_deploy.jar
```

## Usage

`java -jar Main_deploy.jar`

```bash
Usage: <main class> [options] [command] [command options]
  Options:
    --help, h

  Commands:
    enforce      Enforce given configuration
      Usage: enforce [options] /path/to/a/configuration/file
        Options:
          --cluster
            a cluster name, if specified, consolidated (multi-cluster)
            configuration file is expected
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
            run in unsafe mode, deletion is _only_ allowed in this mode
            Default: false

    validate      Validate config
      Usage: validate [options] /path/to/a/configuration/file
        Options:
          --cluster
            a cluster name, if specified, consolidated (multi-cluster)
            configuration file is expected
```

## Configuration
For more on the types of possible quota configurations, and the precedence order of these configurations, 
see [here](https://kafka.apache.org/documentation/#design_quotasconfig) for details.

For the quotas themselves, there are 3 configuration options:
- `producer_byte_rate` and `consumer_byte_rate`
  - for details, see [Network Bandwidth Quotas](https://kafka.apache.org/documentation/#design_quotasbandwidth)
- `request_percentage`
  - for details, see [Request Rate Quota](https://kafka.apache.org/documentation/#design_quotasbandwidth)


#### Sample configuration

```yaml
zookeeper:
  connect: "localhost:22181"
  sessionTimeoutMs: 30000
  connectionTimeoutMs: 30000
    
quotas:
  - principal: user1
    client: clientA
    producer-byte-rate: 5000
    consumer-byte-rate: 4000
    request-percentage: 400
```

OR,

```yaml
zookeeper:
  connect: "localhost:22181"
  sessionTimeoutMs: 30000
  connectionTimeoutMs: 30000
    
quotasFile: /quotas.yaml
```

where `/quotas.yaml` has

```yaml
- principal: user1
  client: clientA
  producer-byte-rate: 5000
  consumer-byte-rate: 4000
  request-percentage: 4000

- principal: user2
  producer-byte-rate: 7000
```

## Multi cluster configuration
To avoid repetition across clusters, the quota enforcer supports multi cluster configuration as well. A sample is shown below,

```yaml
zookeeper:
  connect: "localhost:22181"
  sessionTimeoutMs: 30000
  connectionTimeoutMs: 30000
quotas:
  - principal: user1
    client: clientA
    producer-byte-rate: 5000
    consumer-byte-rate: 4000
    request-percentage: 4000
    clusters:
      foo: {}
      bar:
        producer-byte-rate: 3000
```

To use this feature, use `cluster` flag in the enforcer. Only quotas relevant to the given cluster would be enforced by the enforcer. Other rules are,

* A quota applies to a cluster if that cluster is present under quota's `clusters`
attribute. If no changes to base config are desired, specify empty dict `{}`
* Any quota value can be overridden. Quota 'principal' and 'client' properties are not recommended to be overridden.
* Map (dictionary) properties are merged in a way that preserves base settings
 

Note: `quotasFile` supports multi-cluster configuration as well.

# Defaults

At the top-level of the configuration (where you put `zookeeper` and `quotas` or `quotasFile`) we also support setting
`defaults` for either all quotas or per-cluster. For example:
```yaml
zookeeper:
  connect: "localhost:22181"
  sessionTimeoutMs: 30000
  connectionTimeoutMs: 30000

quotasFile: my-quotas.yaml

defaults:
  producer-byte-rate: 4000
  clusters:
    foo:
      consumer-byte-rate: 2000
```

Would default to a 4000 `producer-byte-rate` quota for all quotas in all clusters, and a 2000 `consumer-byte-rate` for all
 quotas in cluster `foo`. The defaults take similar precedence to the quota-level cluster overrides; that is, the priority is:
 
  * cluster-quota overrides
  * quota configs
  * cluster default
  * default 

# Setting 'entity-default' quotas

"Default" in this context is different from how it is used in the section above. In that section, we allow defining 
a default quota value such that it can simplify your quota configuration file.

Here, a "default" refers to an actual default quota configuration which is stored behind the scenes in Zookeeper, 
and is applied as a "default" to matching clients, where override quotas are not specified. 
You can read more about it here: https://kafka.apache.org/documentation/#design_quotas

To specify a default quota, simply use `<default>` as the value in the quota's `principal` or `client` config fields.
For example, to set the default quota for each unique principal - regardless of the client - you could use:
```yaml
- principal: <default>
  producer-byte-rate: 5000
  consumer-byte-rate: 4000
```

Setting `<default>` is functionally the same as using `--entity-default` in the `kafka-configs.sh` examples
here: https://kafka.apache.org/documentation/#quotas