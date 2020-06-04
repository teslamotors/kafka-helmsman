# Kakfa Roller

Restarting Kafka induces huge toil on the operator, if you do this exercise quite often (config changes, upgrades etc.) – a robust, reliable & cheap to operate implementation can accelerate the rate at which one can push changes to Kafka.

Kafka roller does exactly that -- it allows you to restart any size cluster with a single touch.


## Features

* Restart all nodes in the cluster
* Restart a sub-set of nodes in the cluster
* Keep a watch on the health of the cluster & consumers, throttle if needed
* Custom pre-stop command to be executed before each broker restart

## Dependencies and assumptions

* Python
* Kafka brokers are running on bare metal or VMs
* Kafka brokers are managed via an init system like `systemd` or `upstart`
* Password less SSH to all the brokers

## Installation

```
pip install git://github.com/teslamotors/kafka-helmsman.git#egg=kafka_roller\&subdirectory=kafka_roller
```

OR, from source

```
cd kafka_roller; python setup.py install
```

> Note: Publishing a pypi package is on the roadmap.

## Usage

```bash
Usage: kafka_roller [--core-opts] restart [--options] [other tasks here ...]

Docstring:
  Gracefully restart an instance.

Options:
  -b STRING, --bootstrap=STRING       one or more kafka brokers
  -c STRING, --conf=STRING            kafka admin config overrides in a json file
  -e INT, --healthcheck-wait=INT
  -h INT, --healthcheck-retries=INT
  -p STRING, --pre-stop=STRING
  -r STRING, --pre-start=STRING
  -z STRING, --zk=STRING              zookeeper used by kafka
```

Kafka roller is built on top of `fabric` which itself is based on `pyinvoke`, `--core-opts` are invoke's flags. See full list [here](http://docs.pyinvoke.org/en/latest/invoke.html#core-options-and-flags). Fabric adds some additional core options, see them [here](http://docs.fabfile.org/en/latest/cli.html). Fabric's options are useful to manipulate SSH behavior.

## Sample invocations

#### Restart entire cluster (cluster supports plaintext protocol, i.e. non-secure)

```
/opt/kafka_tools/venv/bin/kafka_roller \
	--prompt-for-passphrase restart \
	--bootstrap=broker1.net:9092,broker2.net:9092 --zk=zk1.net/kafka
```

* `prompt-for-passphrase` causes cli to prompt for SSH passphrase
* zookeeper connection should be the same as the one used by brokers
* The brokers are restarted in an order such that coordinator is the last node to restart

#### Restart entire cluster with a 'pre-stop' command executed before each broker restart

```
/opt/kafka_tools/venv/bin/kafka_roller \
    --prompt-for-passphrase restart \
    --bootstrap=broker1.net:9092,broker2.net:9092 \
    --zk=zk1.net/kafka \
    --pre-stop='/opt/kafka_tools/venv/bin/consumer_health -w 60 -r 60 http://burrow/v3/kafka/test-cluster test-consumer'
```

* `pre-stop` takes a shell command as input, the command is invoked on each broker before the restart. In this example, the command `consumer_health` checks the health of a list of consumers via burrow. It exits with SUCCESS if all the consumers are healthy, if consumers remain unhealthy after enough retries, it exits with a failure code. A non-zero exit from a pre-stop command aborts the rolling restart.
* `consumer_health` command is packaged with kafka_roller, if installed -- you can use this to launch a rolling restart which would make sure consumer(s) stay healthy as restart progresses. Note that a working burrow setup is required for `consumer_health` to work.
* You can set any other command too, for example `sleep 60` would make each broker wait for a minute before proceeding with a restart.

#### Restart a fixed set of hosts

```
/opt/kafka_tools/venv/bin/kafka_roller --prompt-for-passphrase \
    --hosts=broker1.net restart \
    --bootstrap=broker1.net:9092,broker2.net:9092 --zk=zk1.net/kafka
```

* Brokers to be restarted are specified via `hosts` flag, which is a comma-separated string listing hostnames.
* Unlike a full restart where coordinator is put at the last, no re-ordering is performed on the supplied list.


#### Restart a cluster where security is enabled

```
/opt/kafka_tools/venv/bin/kafka_roller \
	--prompt-for-passphrase restart \
	--bootstrap=broker1.net:9092,broker2.net:9092 --zk=zk1.net/kafka --conf /etc/kafka/librdkafka_client.properties
```

* `conf` takes standard librdkafka properties file as input which can have SSL settings.


## Password less SSH setup

Kafka roller restarts the nodes in a sequence by performing SSH login to each broker and issuing init commands to `kafka` service. The easiest way to accomplish this is to use a dedicated user account and configure [password-less ssh](https://www.redhat.com/sysadmin/passwordless-ssh) from a gateway node to all kafka brokers. For example, if `gateway.example.net` is the SSH gateway server and `b1.example.net`, `b2.example.net`, and `b3.example.net` are the three brokers in the cluster.

* Create a user account, let's call it `kafkatools`, on all these four hosts (brokers + gateway)
* Setup password-less SSH for `kafkatools` to allow it to ssh from gateway host to each broker
* Test passwordless SSH from the gateway node `$ ssh b1.example.net`

**NOTE**: Make sure the private key of `kafkatools` user is protected via passphrase.

Kafka roller assumes that the `kafka` service is managed by an init system like `systemd` or `upstart`[1]. Invoking `start/stop/restart` commands via init system requires `sudo` access. We recommend setting up limited sudo access for the user account dedicated for kafka roller, `kafkatools`. A limited `sudo` access can setup via `/etc/sudoers`, refer examples [here](https://askubuntu.com/questions/692701/allowing-user-to-run-systemctl-systemd-services-without-password) and [here](https://askubuntu.com/questions/878170/allow-systemd-service-to-be-started-or-stopped-by-non-sudo-user).

**NOTE**: It's generally a good idea to narrow the scope of sudo access. Config management tools such as Ansible make this process trivially simple.

[1] Only `systemd` or `upstart` at the moment, an extension to other init system is low effort change.

## FAQ

**Q: What qualifies as a healthy state for the cluster?**  
A: A cluster is considered to be unhealthy by kafka roller if

* It has one or more offline partitions
* It has one or more under replicated partitions
* The ratio of leader imbalance on one or more brokers is too high (see `leader.imbalance.per.broker.percentage` in Kafka docs)

**Q: What is the role of zookeeper?**  
A: To calculate leader imbalance, we need preferred assignment. Due to a bug, Kafka admin API did not return preferred assignment in the right order (first broker in the list must be the leader) -- as a workaround, kafka roller reads preferred assignment directly from the zookeeper. The bug has been fixed in the latest version & we plan to drop zookeeper dependency at some point.
