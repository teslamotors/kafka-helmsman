"""
Tasks for performing rolling restart on a cluster.

All the methods decorated with 'task' take a fabric connection as a parameter.
Read: http://docs.fabfile.org/en/2.4/api/connection.html
"""
import json
import logging
import os
import sys

from confluent_kafka.admin import AdminClient
from fabric import SerialGroup as Group
from fabric import task

from cluster_utils import brokers, is_cluster_healthy
from host_utils import hostname, service_start, service_stop, run
from zookeeper import ZK

logger = logging.getLogger(__name__)


@task(
    help={
        "bootstrap": "one or more kafka brokers",
        "zk": "zookeeper used by kafka",
        "healthcheck_retries": "health check retries before aborting",
        "healthcheck_wait": "seconds to wait between health checks",
        "conf": "kafka admin config overrides in a json file",
        "pre_stop": "command to run on each host before stopping the service",
    }
)
def restart(
    c,
    bootstrap,
    zk,
    healthcheck_retries=40,
    healthcheck_wait=30,
    conf=None,
    pre_stop=None,
):
    """Gracefully restart an instance."""
    admin = _admin_client(bootstrap, conf)
    if hasattr(c, "host"):
        hosts = [c.host]
    else:
        hosts = [b.host for b in brokers(admin)]
        assert hosts, "broker list should not be empty!"
        logger.info("Restarting all %s", ", ".join([hostname(ip) for ip in hosts]))
        input("Press Enter to continue...")
    # connect_kwargs contains common SSH configs for the hosts in the group
    for b in Group(*hosts, connect_kwargs=c.connect_kwargs):
        logger.info("Restarting %s", hostname(b.host))
        _instance_restart(b, admin, zk, healthcheck_retries, healthcheck_wait, pre_stop)


def _instance_restart(c, admin, zk, retries, retry_wait, pre_stop):
    """Gracefully restart an instance."""
    with ZK(zk) as zk_conn:
        if not is_cluster_healthy(admin, zk_conn, retries, retry_wait):
            logger.critical("Cluster is not healthy, aborting!")
            sys.exit(1)
        stop(c, pre_stop)
        start(c)


def _admin_client(bootstrap, conf=None):
    final_conf = {
        "bootstrap.servers": bootstrap,
        "log.connection.close": False,
        # mute most of the librdkafka logging
        "log_level": 2,
    }
    if conf is not None:
        final_conf.update(_load_client_conf(conf))
    logger.debug("admin client config %s", final_conf)
    return AdminClient(final_conf)


def _load_client_conf(conf):
    """Load admin client conf from file or string."""
    if os.path.isfile(conf):
        with open(conf) as f:
            return json.load(f)
    else:
        return json.loads(conf)


def stop(c, pre_stop=None):
    """Gracefully stop an instance."""
    if pre_stop is not None:
        run(c, pre_stop)
    service_stop(c, "kafka")
    # introduce & execute post stop here if needed..


def start(c):
    """Gracefully start an instance."""
    # introduce & execute pre start here if needed..
    service_start(c, "kafka")
    # introduce & execute post start here if needed..
