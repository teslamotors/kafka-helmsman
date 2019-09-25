"""Zookeeper module."""

from kazoo.client import KazooClient
from kazoo.retry import KazooRetry


class ZK:
    """Opens a connection to a zookeeper."""

    def __init__(self, hosts):
        """Constructor."""
        self.hosts = hosts

    def __enter__(self):
        """Initialize zk connnection."""
        kazooRetry = KazooRetry(max_tries=5)
        self.zk = KazooClient(
            hosts=self.hosts, read_only=True, connection_retry=kazooRetry
        )
        self.zk.start()
        return self

    def __exit__(self, type, value, traceback):
        """Close zk connnection."""
        self.zk.stop()

    def get(self, path):
        """Return the data of the specified node."""
        data, _ = self.zk.get(path)
        return data
