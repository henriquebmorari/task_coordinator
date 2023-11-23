from kazoo.client import KazooClient
from kazoo.protocol.states import KazooState

class KazooCoordClient(KazooClient):
    def __init__(self, hosts):
        KazooClient.__init__(self, hosts)
        self.start()
