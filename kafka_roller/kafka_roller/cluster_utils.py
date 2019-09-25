"""Kafka cluster utility methods."""
import json
import logging
import time
from collections import defaultdict

from confluent_kafka import KafkaException

# Admin meta data rpc all timeout in milliseconds
META_CALL_TIMEOUT = 50
# leader.imbalance.per.broker.percentage
LEADERSHIP_IMBALANCE_THRESHOLD = 10

logger = logging.getLogger(__name__)


def brokers(admin):
    """
    Return a list of brokers in the cluster.

    Controller is the first item in the list.
    """
    m = _request_meta(admin)
    if m:
        ret = [b for b in m.brokers.values() if b.id != m.controller_id]
        ret.append(m.brokers[m.controller_id])
        return ret
    else:
        return []


def is_cluster_healthy(admin, zk, retries=10, retry_wait=30):
    """Return true if cluster is healthy."""
    retries_left = retries
    while retries_left:
        md = _request_meta(admin)
        if md is not None and not _unhealthy(md, zk):
            logger.info("Cluster is healthy!")
            return True
        else:
            logger.warning(
                "Cluster is not healthy, retries left/total = %d/%d.",
                retries_left,
                retries,
            )
            time.sleep(retry_wait)
            retries_left = retries_left - 1
    logger.error("Cluster was found to be un-healthy after multiple retries.")
    return False


def _unhealthy(md, zk):
    offline, under_rep, imbalanced = (
        offline_partitions(None, md),
        under_replicated_partitions(None, md),
        imbalanced_brokers(None, zk, md),
    )
    unhealthy = offline or under_rep or imbalanced
    if unhealthy:
        logger.warning(
            "%d offline & %d under replicated partitions", len(offline), len(under_rep)
        )
        logger.warning("brokers with imbalanced leadership %s", imbalanced)
    return unhealthy


def under_replicated_partitions(admin, md=None):
    """Return a list of under replicated partitions."""
    if md is None:
        md = _request_meta(admin)
    return [
        (t, p)
        for t in md.topics.values()
        for p in t.partitions.values()
        if len(p.replicas) != len(p.isrs)
    ]


def offline_partitions(admin, md=None):
    """Return a list of offline partitions."""
    if md is None:
        md = _request_meta(admin)
    return [
        (t, p)
        for t in md.topics.values()
        for p in t.partitions.values()
        if len(p.isrs) == 0
    ]


def imbalanced_brokers(admin, zk, md=None, threshold=LEADERSHIP_IMBALANCE_THRESHOLD):
    """Return brokers for which leader imbalance ratio is too high."""
    # This routine uses both metadata retrieved via admin call &
    # one retrieved directly from zookeeper. We are forced to access
    # zookeeper because admin api doesn't guarantee that the replica
    # list will follow any order. That order is required to determine
    # if the current leader is the preferred leader. To get around this,
    # we get the assignment from zookeeper directly.
    if md is None:
        md = _request_meta(admin)
    from_zk = _assignment_from_zk(zk, md.topics.keys())
    preferred = _preferred_assignment(from_zk)
    unfavorable = _unfavorable_assignment(md, from_zk)
    return [
        (b, float(len(unfavorable[b])) / len(preferred[b]) * 100)
        for b in preferred
        if b in unfavorable
        and float(len(unfavorable[b])) / len(preferred[b]) * 100 > threshold
    ]


def _unfavorable_assignment(md, assignment_from_zk):
    """Return a map of broker to a list of unfavorably assigned partitions."""
    # An assignment is unfavorable if the preferred broker for partition
    # is not its leader.
    result = defaultdict(set)
    for topic, assignment in assignment_from_zk.items():
        for partition, replicas in assignment.items():
            # first broker in the replica list is the preferred leader
            preferred = int(replicas[0])
            partition_info_from_md = md.topics[topic].partitions[int(partition)]
            actual = partition_info_from_md.leader
            if preferred != actual:
                result[preferred].add((topic, int(partition)))
    return result


def _preferred_assignment(assignment_from_zk):
    """Return a map of broker to a list of preferred partition assignment."""
    # The output of this method indicates how the assignment should be ideally
    result = defaultdict(set)
    for topic, assignment in assignment_from_zk.items():
        for partition, replicas in assignment.items():
            # first broker in the replica list is the preferred leader
            preferred = int(replicas[0])
            result[preferred].add((topic, int(partition)))
    return result


def _assignment_from_zk(zk, topics):
    """Fetch assignment as stored in zookeeper.

    Kafka stores its assignment for a topic at /brokers/topics/[topic]
    full details of schema here: https://goo.gl/MKjQ8E

    Example:
    {
        "version": 1,
        "partitions": {"0": [0, 1, 3] } }
    }
    """
    result = dict()
    for t in topics:
        path = "/brokers/topics/{}".format(t)
        result[t] = json.loads(zk.get(path).decode())["partitions"]
    return result


def _request_meta(admin):
    """Fetch cluster meta data."""
    md = None
    try:
        md = admin.list_topics(timeout=META_CALL_TIMEOUT)
    except KafkaException:
        logger.exception("Failed to fetch metadata")
    if md is not None:
        logger.debug("Fetched cluster metadata successfully!")
    return md
