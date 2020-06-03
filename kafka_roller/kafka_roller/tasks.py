"""
Tasks for performing rolling restart on a cluster.

All the methods decorated with 'task' take a fabric connection as a parameter.
Read: http://docs.fabfile.org/en/2.4/api/connection.html
"""
import json
import logging
import os
import sys
from datetime import datetime

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
        "pre_start": "command to run on each host before starting the service",
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
    pre_start=None,
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
    hosts = Group(*hosts, connect_kwargs=c.connect_kwargs)
    for index, b in enumerate(hosts):
        logger.info("(%s/%s) Restarting %s", index + 1, len(hosts), hostname(b.host))
        start = datetime.now()
        _instance_restart(
            b, admin, zk, healthcheck_retries, healthcheck_wait, pre_stop, pre_start
        )
        end = datetime.now()
        logger.info("Instance restart took: %s", end - start)


def _instance_restart(c, admin, zk, retries, retry_wait, pre_stop, pre_start):
    """Gracefully restart an instance."""
    with ZK(zk) as zk_conn:
        if not is_cluster_healthy(admin, zk_conn, retries, retry_wait):
            logger.critical("Cluster is not healthy, aborting!")
            sys.exit(1)
        stop(c, pre_stop)
        start(c, pre_start)


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


def start(c, pre_start=None):
    """Gracefully start an instance."""
    if pre_start is not None:
        result = run(c, pre_start)
        if result.return_code != 0:
            logger.error(
                "Pre-start command {} failed with error: {}",
                result.command,
                result.return_code,
            )
            sys.exit(1)
    service_start(c, "kafka")
    # introduce & execute post start here if needed..
