# kafka-helmsman

kafka-helmsman is a repository of tools that focus on automating a Kafka deployment.
These tools were developed by data platform engineers at Tesla, they add value to the open-source Kafka ecosystem in a couple of ways:

1. The tasks covered by these tools are infamous for adding toil on engineers, for us these tools save engineering weeks each quarter
2. They have been battle-tested internally, high quality and user-friendly

The tools are

* [**Kafka consumer freshness tracker**](kafka_consumer_freshness_tracker/README.md)
* [**Kafka topic enforcer**](kafka_topic_enforcer/README.md)
* [**Kafka roller**](kafka_roller/README.md)

Follow the links for more detail.

# Development

The tools are written in Java & Python. Refer to language specific section for development instructions.

## Java

### Dependencies

Java code uses [bazel](https://www.bazel.build) as the build tool. If you don't have `bazel` installed, follow the instructions [here](https://docs.bazel.build/versions/master/install.html). 

At the time of writing the Bazel version used is `0.24.1`.

### Test

```
bazel test //...:all
```

### Build

```
bazel build //...:all
```

> Note: In bazel, target spec `//..:all` indicates all rules in all packages beneath workspace, thus `bazel build //...:all` implies build everything.

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
