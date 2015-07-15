from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor

class ClusterProtocol(Protocol):
    def __init__(self, data):
        self.data = data
        self.state = 0
    def connectionMade(self):
        self.transport.write(str(self.data))
        self.state += 1
    def connectionLost(self, reason):
        pass
    def dataReceived(self, data):
        pass

class ClusterFactory(Factory):
    def __init__(self):
        self.connectionNumber = -1
    def buildProtocol(self, addr):
        self.connectionNumber += 1
        return ClusterProtocol(self.connectionNumber)
reactor.listenTCP(9000, ClusterFactory())
reactor.run()
