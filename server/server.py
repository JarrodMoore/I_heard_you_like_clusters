from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
import os

def read_in_chunks(f=GLOBAL_FILE_IN, chunk_size=50):
    data = ''
    for line_number in range(chunk_size):
        data += f.readline()
    old_file_position = f.tell()
    f.seek(0, os.SEEK_END)
    size = f.tell() - old_file_position
    f.seek(old_file_position, os.SEEK_SET)
    if size < 1024:
        data += f.read()
    return data

class ClusterProtocol(Protocol):
    def __init__(self, data):
        self.connectionNumber = data
    def connectionMade(self):
        self.clusterWorkSend()
    def connectionLost(self, reason):
        pass
    def dataReceived(self, data):
        if isinstance(GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber], basestring) #FINAL CENTERS
            GLOBAL_FINAL_CENTERS[int(GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber][:-1])] = data
        else: #DATA TO CLUSTER FOR FINAL CENTERS
            if GLOBAL_CENTERS[GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]] == None: #FIRST DATA FOR ITER RECIEVED
                GLOBAL_CENTERS[GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]] = data
            else: #DATA FOR ITER ALREADY RECIEVED
                GLOBAL_CENTERS[GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]] = GLOBAL_CENTERS[GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]] + ',' + data
    def clusterWorkSend(self):
        data = read_in_chunks()
        if data == '': #ALL DATA FOR ITER SENT
            if GLOBAL_CLUSTER_ITER >= GLOBAL_ITER_LIMIT: #ALL ITERATIONS STARTED
                GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]  = None
                if GLOBAL_ITER_LIMIT -1 in GLOABL_CLUSTER_ITER_NUMBER: #DATA IN FINAL ITER STILL BEING PROCESSED
                    self.loseConnection()
                else: #ALL DATA IN FINAL ITER PROCESSED
                    GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(currentIterNumberTemp) + 'F'
                    self.transport.write(GLOBAL_CENTERS[GLOBAL_ITER_LIMIT-1])
            else: #MORE ITERS TO DO
                currentIterNumberTemp = GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]
                GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]  = None
                if currentIterNumberTemp in GLOABL_CLUSTER_ITER_NUMBER: #DATA IN CURRENT ITER STILL BEING PROCESSED
                    GLOBAL_CLUSTER_ITER +=1
                    GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                    GLOBAL_FILE_IN.seek(0)
                    data = read_in_chunks()
                    self.transport.write(data)
                else: #ALL DATA FOR CURRENT ITER PROCESSED
                    GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(currentIterNumberTemp) + 'F'
                    data = GLOBAL_CENTERS[currentIterNumberTemp]
                    self.transport.write(data)
        else: #PROCESS DATA IN CURRENT ITER
           GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
           self.transport.write(data)
class ClusterFactory(Factory):
    def __init__(self):
        self.connectionNumber = -1
    def buildProtocol(self, addr):
        self.connectionNumber += 1
        return ClusterProtocol(self.connectionNumber)
if __name__ == "__main__":
    GLOBAL_FILE_IN = open("house-votes-84.data")
    GLOBAL_CLUSTER_ITER = 0
    GLOBAL_ITER_LIMIT = 1000
    GLOBAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
    GLOBAL_FINAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
    GLOBAL_CLIENT_ITER_NUMBER = GLOBAL_ITER_LIMIT*[None]
    reactor.listenTCP(9000, ClusterFactory())
    reactor.run()
