from kazoo.client import KazooClient

class ZookeeperClient(KazooClient):
    def __init__(self, hosts):
        KazooClient.__init__(self, hosts)
        self.start()
