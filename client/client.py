# Copyright (c) Twisted Matrix Laboratories.
# See LICENSE for details.


"""
An example client. Run simpleserv.py first before running this.
"""

from twisted.internet import reactor, protocol
import pickle
import numpy as np
from sklearn.cluster import KMeans
import StringIO
import time
# a client protocol

class EchoClient(protocol.Protocol):
    
    def connectionMade(self):
        self.STATE = "GET K"
        self.holdData = ''
        print "Yo, I hear you like to cluster"
    
    def dataReceived(self, data):
        if self.STATE == "GET K":
            self.clusterWorker = KMeans(n_clusters = int(data))
            self.transport.write('Recieved K')
            self.STATE = "CLUSTER"
        elif self.STATE == "CLUSTER":
            data = data.replace(' ', '')
            data = data.replace('\n,', '\n')
            
            data_stream = StringIO.StringIO(data)
            M = np.genfromtxt(data_stream, delimiter=',')
            if not np.all(np.isfinite(M)):
                self.transport.writ("NON FINITE DATA SENT")
                self.transport.loseConnection()
            else:
                self.clusterWorker.fit(M)
                centers_np = self.clusterWorker.cluster_centers_
                centers_string = ''
#                print len(centers_np[0])
                for cent in centers_np:
                    for c in cent:
                        centers_string += str(c) +','
#                        print "CURRENT CENTER STRING :", centers_string
                    centers_string =  centers_string[:-1] + '\n'
#                print "SENT: ", centers_string
                self.transport.write(centers_string)
        
    
    def connectionLost(self, reason):
        pass

class EchoFactory(protocol.ClientFactory):
    protocol = EchoClient

    def clientConnectionFailed(self, connector, reason):
        print "So I clustered on a cluster"
        reactor.stop()
    
    def clientConnectionLost(self, connector, reason):
        print "So I clustered on a cluster"
        reactor.stop()


# this connects the protocol to a server running on port 8000
def main():
    f = EchoFactory()
    try:
        reactor.connectTCP("localhost", 9000, f)
        reactor.run()
    except:    
        time.sleep(1)
# this only runs if the module was *not* imported
if __name__ == '__main__':
    main()
