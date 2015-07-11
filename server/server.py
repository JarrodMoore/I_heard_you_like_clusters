from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

class ClusterProtocol(LineReceiver):
    def __init__(self, data):
        self.data = data
        self.state = None
    def connectionMade(self):
        self.sendLine("YA I WORK")
    def connectionLost(self, reason):
        pass
    def lineReveived(self, line):
        pass

class ClusterFactory(Factory):
    def __init__(self):
        pass
    def buildProtocol(self, addr):
        return ClusterProtocol(None)
reactor.listenTCP(9000, ClusterFactory())
reactor.run()
