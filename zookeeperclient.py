from kazoo.client import KazooClient

class ZookeeperClient(KazooClient):
    """Simple zookeeper client."""

    def __init__(self, hosts):
        KazooClient.__init__(self, hosts)
        self.start()
