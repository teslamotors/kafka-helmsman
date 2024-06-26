# kafka-helmsman
![Build status](https://github.com/teslamotors/kafka-helmsman/workflows/CI/badge.svg)

kafka-helmsman is a repository of tools that focus on automating a Kafka deployment.
These tools were developed by data platform engineers at Tesla, they add value to the open-source Kafka ecosystem in a couple of ways:

1. The tasks covered by these tools are infamous for adding toil on engineers, for us these tools save engineering weeks each quarter
2. They have been battle-tested internally, high quality and user-friendly

The tools are

* [**Kafka consumer freshness tracker**](kafka_consumer_freshness_tracker/README.md)
* [**Kafka topic enforcer**](kafka_topic_enforcer/README.md)
* [**Kafka roller**](kafka_roller/README.md)
* [**Kafka quota enforcer**](kafka_quota_enforcer/README.md)

Follow the links for more detail.

# Development

The tools are written in Java & Python. Refer to language specific section for development instructions.

## Java

Kafka Helmsman is built and tested using Java 17. Bazel hermetically fetches a Java 17 distribution and uses it to compile the code and run tests.
User environments only require the minimum Java version required for running Bazel itself (currently Java 8).

### Dependencies

Java code uses [bazel](https://www.bazel.build) as the build tool, installation of which is managed via bazelisk. Bazelisk is a version manager for Bazel. It takes care of downloading and installing Bazel itself, so you don’t have to worry about using the correct version of Bazel.

Bazelisk can be installed in different ways, see [here](https://docs.bazel.build/versions/master/install-bazelisk.html) for details.

### Test

```
bazel test //...
```

### Build

```
bazel build //...
```

> Note: In bazel, target spec `//...` indicates all targets in all packages beneath workspace, thus `bazel build //...` implies build everything.

### Intellij

If you are using Intellij + Bazel, you can import the project directly with:
 * File -> Import Bazel Project
 * Select kafka-helmsman from where you cloned it locally
 * Select "Import project view file" and select the `ij.bazelproject` file
 * Select "Finish" with Infer from: Workspace (the default selection)

We recommend using the latest version of Intellij with the latest version of Bazel plugin.

## Python

### Dependencies

Python code uses [tox](https://tox.readthedocs.io/en/latest/) to run tests. If you don't have `tox` installed, here is a quick primer of tox. 


#### **Tox Primer** (_optional, skip if you have a working tox setup_)
##### Install pyenv
```
brew install pyenv
brew install pyenv-virtualenv
```
##### Install python versions
```
pyenv install 3.5.7
pyenv install 3.6.9
pyenv install 3.7.4
```
##### Install tox
```
pyenv virtualenv 3.7.4 tox
pyenv activate tox
pip install tox
pyenv deactivate
```
##### Setup python path
```
pyenv local 3.5.7 3.6.9 3.7.4 tox
```

### Test

```
./build_python.sh
```

### Build

```
./build_python.sh package
```
