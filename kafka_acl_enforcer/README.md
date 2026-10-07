# Kafka ACL Enforcer

Kafka ACL enforcer keeps the ACLs of a Kafka cluster in line with a version controlled configuration. Configured
ACLs missing from the cluster are created, ACLs present on the cluster but absent from the configuration are
deleted (deletion only happens in `--unsafemode`).

## Installation

```
bazel build //kafka_acl_enforcer/src/main/java/com/tesla/data/acl:Main_deploy.jar
```

## Usage

```
java -jar Main_deploy.jar validate /path/to/config.yaml
java -jar Main_deploy.jar enforce [options] /path/to/config.yaml
```

`enforce` supports the same options as the topic enforcer: `--cluster`, `--continuous`, `--dryrun`, `--interval`
and `--unsafemode`.

## Configuration

```yaml
kafka:
  bootstrap.servers: "broker1:9092,broker2:9092,broker3:9092"
acls:
  - resource:
      type: TOPIC
      name: orders
      pattern: LITERAL
    entry:
      principal: User:orders-service
      host: "*"
      operation: WRITE
      permission: ALLOW
```

`aclsFile: /path/to/acls.yaml` can be used instead of `acls`.

## Unmanaged resources

ACLs on resources owned by another system, for example a third-party tool that manages its own topics and their ACLs,
can be excluded from deletion by resource name prefix:

```yaml
unmanaged:
  topicPrefixes: ["ext-", "vendor-"]   # TOPIC ACLs whose resource name starts with any of these
  groupPrefixes: ["ext-"]              # GROUP ACLs whose resource name starts with any of these
```

* Existing ACLs on matching resources are never deleted and are not reported as unexpected.
* Configured ACLs on matching resources are still created. Removing such an ACL from the configuration does **not**
  delete it from the cluster, the owning system is responsible for cleaning it up.
* ACLs on the wildcard resource `*` are always enforced.
* A PREFIXED ACL is unmanaged only if its own name starts with a configured prefix. With prefix `ext-`, a PREFIXED
  ACL on `ext` is still enforced, since it covers more than the unmanaged namespace.
* CLUSTER and TRANSACTIONAL_ID ACLs are always enforced.
* Unlike the topic enforcer, `_` is not excluded unless it is listed.
* Prefixes must be non-empty and have no surrounding whitespace. Unknown keys under `unmanaged` fail the run.
* A misspelled section name (ex: `unmanged`) is not detected. The enforcer logs the effective prefixes at startup
  (`Unmanaged topic prefixes: [...], group prefixes: [...]`), check that line when rolling out a new configuration.
