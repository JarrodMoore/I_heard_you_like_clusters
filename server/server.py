from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
import os
import numpy as np
import sys

GLOBAL_FILE_IN = open(sys.argv[1])
GLOBAL_CLUSTER_ITER = 0
GLOBAL_ITER_LIMIT = 100
GLOBAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
GLOBAL_FINAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
GLOBAL_CLUSTER_ITER_NUMBER = GLOBAL_ITER_LIMIT*[None]

def read_in_chunks(f=GLOBAL_FILE_IN, chunk_size=100):
    data = ''
    for line_number in range(chunk_size):
        data += f.readline()
    old_file_position = f.tell()
    f.seek(0, os.SEEK_END)
    size = f.tell() - old_file_position
    f.seek(old_file_position, os.SEEK_SET)
    if size < 2048:
        data += f.read()
    return data

class ClusterProtocol(Protocol):
    def __init__(self, data):
        self.connectionNumber = data
    def connectionMade(self):
        print "CONNECTION MADE"
        self.transport.write(sys.argv[2])
    def connectionLost(self, reason):
        pass
    def dataReceived(self, data):
        global GLOBAL_CLUSTER_ITER
        global GLOBAL_ITER_LIMIT
        global GLOBAL_CLUSTER_ITER
        global GLOBAL_CENTERS
        global GLOBAL_FINAL_CENTERS
        global GLOBAL_CLUSTER_ITER_NUMBER
        print GLOBAL_CLUSTER_ITER
        if GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] == None:
            GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
            self.clusterWorkSend()
        elif isinstance(GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber], basestring): #FINAL CENTERS
            GLOBAL_FINAL_CENTERS[int(GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber][:-1])] = data
            self.clusterWorkSend()
        else: #DATA TO CLUSTER FOR FINAL CENTERS
#            print GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]
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
        currentIterNumberTemp = GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]
        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]  = None
        if data == '': #ALL DATA FOR ITER SENT
#            print "IF 0"
            if GLOBAL_CLUSTER_ITER == GLOBAL_ITER_LIMIT-1: #ALL ITERATIONS STARTED
#                print "IF 0-0"
                if GLOBAL_ITER_LIMIT -1 in GLOBAL_CLUSTER_ITER_NUMBER: #DATA IN FINAL ITER STILL BEING PROCESSED
#                    print "IF 0-0-0"
#                    done = True
#                    for cents, index in zip(GLOBAL_FINAL_CENTERS, range(len(GLOBAL_FINAL_CENTERS))):
#                        if cents == None:
#                            done = False
#                            GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(index) + 'F'
#                            self.transport.write(GLOBAL_CENTERS[index])
#                    if done:
                    print "CONNECTION # " , self.connectionNumber, " lost"
                    self.transport.loseConnection()
                else: #ALL DATA IN FINAL ITER PROCESSED
#                    print "IF 0-0-1"
                    if isinstance(currentIterNumberTemp, basestring): #FINAL CENTERS CLUSTERED
#                        print "IF 0-0-1-0"
                        done = True
#                        for cents, index in zip(GLOBAL_FINAL_CENTERS, range(len(GLOBAL_FINAL_CENTERS))):
#                            if cents == None:
#                                done = False
#                                GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(index) + 'F'
#                                self.transport.write(GLOBAL_CENTERS[index])
                        if done:
                            print "REACTOR STOPPED"
                            reactor.stop()
                    else: #CLUSTER FINAL CENTERS
#                        print "IF 0-0-1-1"
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(currentIterNumberTemp) + 'F'
                        self.transport.write(GLOBAL_CENTERS[GLOBAL_ITER_LIMIT-1])
            else: #MORE ITERS TO DO
#                print "IF 0-1"
                if currentIterNumberTemp in GLOBAL_CLUSTER_ITER_NUMBER: #DATA IN CURRENT ITER STILL BEING PROCESSED
#                    print "IF 0-1-0"
                    if currentIterNumberTemp + 1 not in GLOBAL_CLUSTER_ITER_NUMBER:
                        GLOBAL_CLUSTER_ITER += 1
                    GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                    GLOBAL_FILE_IN.seek(0)
                    data = read_in_chunks()
                    self.transport.write(data)
                else: #ALL DATA FOR CURRENT ITER PROCESSED
#                    print "IF 0-1-1"
                    if isinstance(currentIterNumberTemp, basestring): #FINAL CENTERS CLUSTERED
#                        print "IF 0-1-1-0"
#                        GLOBAL_FINAL_CENTERS[int(currentIterNumberTemp[:-1])] = data
                        if int(currentIterNumberTemp[:-1]) + 1 not in GLOBAL_CLUSTER_ITER_NUMBER:
                            GLOBAL_CLUSTER_ITER +=1
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                        GLOBAL_FILE_IN.seek(0)
                        data = read_in_chunks()
                        self.transport.write(data)
                    else:
#                        print "IF 0-1-1-1"
                        print "CLUSTERING FINAL CENTERS"
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(currentIterNumberTemp) + 'F'
                        data = GLOBAL_CENTERS[currentIterNumberTemp]
                        self.transport.write(data)
        else: #PROCESS DATA IN CURRENT ITER
#           print "IF 1"
            if (GLOBAL_CLUSTER_ITER -1 not in GLOBAL_CLUSTER_ITER_NUMBER) and (GLOBAL_FINAL_CENTERS[GLOBAL_CLUSTER_ITER -1] == None) and (GLOBAL_CLUSTER_ITER != 0) and not (isinstance(currentIterNumberTemp, basestring)): #FINAL CENTERS CLUSTERED
                GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(currentIterNumberTemp) + 'F'
                data = GLOBAL_CENTERS[currentIterNumberTemp]
                self.transport.write(data)
            else:
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
#    for cent, index in zip(GLOBAL_CENTERS, range(len(GLOBAL_CENTERS))):
#        print len(cent)
#        if len(cent) != 4211:
#            print index, GLOBAL_FINAL_CENTERS[index]
