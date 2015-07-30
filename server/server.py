from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
import os
import numpy as np
import sys

GLOBAL_FILE_IN = open("house-votes-84.data")
GLOBAL_CLUSTER_ITER = 0
GLOBAL_ITER_LIMIT = 10
GLOBAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
GLOBAL_FINAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
GLOBAL_CLUSTER_ITER_NUMBER = GLOBAL_ITER_LIMIT*[None]

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
        self.transport.write(sys.argv[1])
    def connectionLost(self, reason):
        pass
    def dataReceived(self, data):
        global GLOBAL_CLUSTER_ITER
        global GLOBAL_ITER_LIMIT
        global GLOBAL_CLUSTER_ITER
        global GLOBAL_CENTERS
        global GLOBAL_FINAL_CENTERS
        global GLOBAL_CLUSTER_ITER_NUMBER
        if GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] == None:
            self.clusterWorkSend()
        elif isinstance(GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber], basestring): #FINAL CENTERS
            GLOBAL_FINAL_CENTERS[int(GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber][:-1])-1] = data
            self.clusterWorkSend()
        else: #DATA TO CLUSTER FOR FINAL CENTERS
            print GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]
            if GLOBAL_CENTERS[GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]] == None: #FIRST DATA FOR ITER RECIEVED
                GLOBAL_CENTERS[GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]] = data
                self.clusterWorkSend()
            else: #DATA FOR ITER ALREADY RECIEVED
                GLOBAL_CENTERS[GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]] = GLOBAL_CENTERS[GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]] + ',' + data
                self.clusterWorkSend()
    def clusterWorkSend(self):
        global GLOBAL_CLUSTER_ITER
        global GLOBAL_ITER_LIMIT
        global GLOBAL_CLUSTER_ITER
        global GLOBAL_CENTERS
        global GLOBAL_FINAL_CENTERS
        global GLOBAL_CLUSTER_ITER_NUMBER

        data = read_in_chunks()
        if data == '': #ALL DATA FOR ITER SENT
            print "IF 0"
            if GLOBAL_CLUSTER_ITER == GLOBAL_ITER_LIMIT-1: #ALL ITERATIONS STARTED
                print "IF 0-0"
                currentIterNumberTemp = GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]
                GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]  = None
                if GLOBAL_ITER_LIMIT -1 in GLOBAL_CLUSTER_ITER_NUMBER: #DATA IN FINAL ITER STILL BEING PROCESSED
                    print "IF 0-0-0"
                    self.loseConnection()
                else: #ALL DATA IN FINAL ITER PROCESSED
                    print "IF 0-0-1"
                    if isinstance(currentIterNumberTemp, basestring): #FINAL CENTERS CLUSTERED
                        print "IF 0-0-1-0"
                        
                        reactor.stop()
                    else: #CLUSTER FINAL CENTERS
                        print "IF 0-0-1-1"
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(currentIterNumberTemp) + 'F'
                        self.transport.write(GLOBAL_CENTERS[GLOBAL_ITER_LIMIT-1])
            else: #MORE ITERS TO DO
                print "IF 0-1"
                currentIterNumberTemp = GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]
                GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]  = None
                if currentIterNumberTemp in GLOBAL_CLUSTER_ITER_NUMBER: #DATA IN CURRENT ITER STILL BEING PROCESSED
                    print "IF 0-1-0"
                    GLOBAL_CLUSTER_ITER +=1
                    GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                    GLOBAL_FILE_IN.seek(0)
                    data = read_in_chunks()
                    self.transport.write(data)
                else: #ALL DATA FOR CURRENT ITER PROCESSED
                    print "IF 0-1-1"
                    if isinstance(currentIterNumberTemp, basestring): #FINAL CENTERS CLUSTERED
                        print "IF 0-1-1-0"
                        GLOBAL_FINAL_CENTERS[int(currentIterNumberTemp[:-1])] = data
                        GLOBAL_CLUSTER_ITER +=1
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                        GLOBAL_FILE_IN.seek(0)
                        data = read_in_chunks()
                        self.transport.write(data)
                    else:
                        print "IF 0-1-1-1"
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(currentIterNumberTemp) + 'F'
                        data = GLOBAL_CENTERS[currentIterNumberTemp]
                        self.transport.write(data)
        else: #PROCESS DATA IN CURRENT ITER
           print "IF 1"
           GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
           self.transport.write(data)
class ClusterFactory(Factory):
    def __init__(self):
        self.connectionNumber = -1
    def buildProtocol(self, addr):
        self.connectionNumber += 1
        return ClusterProtocol(self.connectionNumber)

if __name__ == "__main__":
#    GLOBAL_FILE_IN = open("house-votes-84.data")
#    GLOBAL_CLUSTER_ITER = 0
#    GLOBAL_ITER_LIMIT = 1000
#    GLOBAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
#    GLOBAL_FINAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
#    GLOBAL_CLIENT_ITER_NUMBER = GLOBAL_ITER_LIMIT*[None]
    reactor.listenTCP(9000, ClusterFactory())
    reactor.run()
    for center in GLOBAL_FINAL_CENTERS:
        print center
