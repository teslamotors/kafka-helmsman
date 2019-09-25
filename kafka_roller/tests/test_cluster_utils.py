from unittest.mock import patch
from kafka_roller import cluster_utils
from confluent_kafka.admin import (
    ClusterMetadata,
    BrokerMetadata,
    TopicMetadata,
    PartitionMetadata,
)


def broker(id, host):
    bm = BrokerMetadata()
    bm.id, bm.host, bm.port = id, host, 9092
    return bm


def topic(name, partitions=[]):
    tm = TopicMetadata()
    tm.topic = name
    tm.partitions = {p.partition: p for p in partitions}
    return tm


def partition(id, leader=-1, replicas=[], isrs=[]):
    pm = PartitionMetadata()
    pm.partition, pm.leader = id, leader
    pm.replicas, pm.isrs = replicas, isrs
    return pm


def cluster_meta(topics):
    brokers = [broker(100, "broker1"), broker(101, "broker2"), broker(102, "broker3")]
    cluster = ClusterMetadata()
    cluster.brokers = {b.id: b for b in brokers}
    cluster.topics = {t.topic: t for t in topics}
    cluster.controller_id = 101
    return cluster


@patch("confluent_kafka.admin.AdminClient")
def test_under_replicated_partitions(mocked_admin):
    t1 = topic(
        "t1",
        [
            partition(0, 100, [100, 102], [100]),
            partition(1, 101, [100, 101], [100, 101]),
        ],
    )
    mocked_admin.list_topics.return_value = cluster_meta([t1])
    result = cluster_utils.under_replicated_partitions(mocked_admin)
    assert len(result) == 1
    assert result[0][1].partition == 0


@patch("confluent_kafka.admin.AdminClient")
def test_offline_partitions(mocked_admin):
    t1 = topic(
        "t1", [partition(0, 100, [100, 102]), partition(1, 101, [100, 101], [100, 101])]
    )
    mocked_admin.list_topics.return_value = cluster_meta([t1])
    result = cluster_utils.offline_partitions(mocked_admin)
    assert len(result) == 1
    assert result[0][1].partition == 0


def test_preferred_assignment():
    from_zk = {"t1": {"0": [100, 102], "1": [100, 101]}, "t2": {"0": [101, 102]}}
    result = cluster_utils._preferred_assignment(from_zk)
    assert len(result) == 2
    assert result[100] == {("t1", 0), ("t1", 1)}
    assert result[101] == {("t2", 0)}


def test_unfavorable_assignment():
    t1 = topic(
        "t1",
        [
            partition(0, leader=100, replicas=[100, 102]),
            partition(1, leader=101, replicas=[100, 101], isrs=[100, 101]),
        ],
    )
    from_zk = {"t1": {"0": [100, 102], "1": [100, 101]}}
    result = cluster_utils._unfavorable_assignment(cluster_meta([t1]), from_zk)
    assert len(result) == 1
    assert result[100] == {("t1", 1)}


@patch("confluent_kafka.admin.AdminClient")
@patch("kafka_roller.zookeeper.ZK")
def test_imbalanced_brokers(mocked_zk, mocked_admin):
    t1 = topic(
        "t1",
        [
            partition(0, leader=100, replicas=[100, 102]),
            partition(1, leader=101, replicas=[100, 101], isrs=[100, 101]),
        ],
    )
    from_zk = """
    {"partitions": {"0": [100, 102], "1": [100, 101]}}
    """.encode()
    mocked_admin.list_topics.return_value = cluster_meta([t1])
    mocked_zk.get.return_value = from_zk
    result = cluster_utils.imbalanced_brokers(mocked_admin, mocked_zk)
    assert len(result) == 1
    assert result == [(100, 50.0)]


@patch("confluent_kafka.admin.AdminClient")
def test_brokers(mocked_admin):
    md = cluster_meta([])
    mocked_admin.list_topics.return_value = md
    result = cluster_utils.brokers(mocked_admin)
    assert len(result) == len(md.brokers)
    assert result[-1].id == md.controller_id
