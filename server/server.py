from twisted.internet.protocol import Factory
from twisted.internet.protocol import Protocol
from twisted.protocols.basic import LineReceiver
from twisted.internet import reactor
import os
import numpy as np
import sys

def checkArgs():
    '''Check to make sure all arguments are provided in command line
    arg1: port, 
    arg2: infile, 
    arg3: oufile, 
    arg4: num centers, 
    arg5: chunk size
    '''
    if len(sys.argv) < 6:
        print "Not enough arguments were provided. Please try again with correct format:"
        print "server.py port infile outfile numCenter chunkSize"
        exit(0)

checkArgs()
try:
    GLOBAL_FILE_IN = open(sys.argv[2])
except:
    print str(sys.argv[2]) + " was unable to be opended."
    exit(0)
GLOBAL_ITER_LIMIT = int(sys.argv[6])
GLOBAL_CLUSTER_ITER = 0
GLOBAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
GLOBAL_FINAL_CENTERS = GLOBAL_ITER_LIMIT*[None]
GLOBAL_CLUSTER_ITER_NUMBER = GLOBAL_ITER_LIMIT*[None]

def read_in_chunks(f=GLOBAL_FILE_IN, chunk_size=int(sys.argv[5])):
    data = ''
    for line_number in range(chunk_size):
        data += f.readline()
    old_file_position = f.tell()
    f.seek(0, os.SEEK_END)
    size = f.tell() - old_file_position
    f.seek(old_file_position, os.SEEK_SET)
    if size < (chunk_size*1024/50):
 #       print size
        data += f.read()
    return data

def check_less_in(n, I):
    for i in range(n):
        if i in I:
            return True
    return False

def check_for_none(n, I):
    for index in range(n):
       if I[index] == None:
           return True
    return False

def find_none(n, I):
    return_array = []
    for index in range(n):
       if I[index] == None:
           return_array.append(index)
    return return_array

def check_for_final(s, A):
    if s in A:
        return True
    return False

class ClusterProtocol(Protocol):
    def __init__(self, data):
        self.connectionNumber = data
    def connectionMade(self):
        self.transport.write(sys.argv[4])
    def connectionLost(self, reason):
        pass
    def dataReceived(self, data):
        global GLOBAL_CLUSTER_ITER
        global GLOBAL_ITER_LIMIT
        global GLOBAL_CLUSTER_ITER
        global GLOBAL_CENTERS
        global GLOBAL_FINAL_CENTERS
        global GLOBAL_CLUSTER_ITER_NUMBER
#        print 'recieved ', GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]
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
#               print "IF 0-0"
                for cents, index in zip(GLOBAL_FINAL_CENTERS, range(len(GLOBAL_FINAL_CENTERS))):
                    if (cents == None) and (str(index) + 'F' not in GLOBAL_CLUSTER_ITER_NUMBER):
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(index) + 'F'
                        self.transport.write(GLOBAL_CENTERS[index])
                        break
                else:
                    for i in range(len(GLOBAL_CLUSTER_ITER_NUMBER)):
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = None
                        if GLOBAL_CLUSTER_ITER_NUMBER[i] != None:
                            self.transport.loseConnection()
                            break
                for cent in GLOBAL_FINAL_CENTERS:
                     if cent == None:
                         break
                else:
                    reactor.stop()
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
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(currentIterNumberTemp) + 'F'
                        data = GLOBAL_CENTERS[currentIterNumberTemp]
                        self.transport.write(data)
        else: #PROCESS DATA IN CURRENT ITER
#           print "IF 1"
            if not (check_less_in(GLOBAL_CLUSTER_ITER -1, GLOBAL_CLUSTER_ITER_NUMBER)):
#                print '(check_less_in(GLOBAL_CLUSTER_ITER -1, GLOBAL_CLUSTER_ITER_NUMBER)) == FALSE'
                if check_for_none(GLOBAL_CLUSTER_ITER - 1, GLOBAL_FINAL_CENTERS):
#                    print 'check_for_none(GLOBAL_CLUSTER_ITER -1, GLOBAL_FINAL_CENTERS) == TRUE'
                    if (GLOBAL_CLUSTER_ITER != 0): #FINAL CENTERS CLUSTERED
#                        print "(GLOBAL_CLUSTER_ITER != 0) == TRUE"
                        index = find_none(GLOBAL_CLUSTER_ITER-1, GLOBAL_FINAL_CENTERS)
                        for i in index:
                            if not str(i) + 'F' in GLOBAL_CLUSTER_ITER_NUMBER:
                                GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = str(i) + 'F'
                                data = GLOBAL_CENTERS[i]
                                self.transport.write(data)
                                break
                        else:
                            GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                            self.transport.write(data)
                    else:
                        GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                        self.transport.write(data)
                else:
                    GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                    self.transport.write(data)
            else:
                GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber] = GLOBAL_CLUSTER_ITER
                self.transport.write(data)
#        print 'sent ', GLOBAL_CLUSTER_ITER_NUMBER[self.connectionNumber]
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
    reactor.listenTCP(int(sys.argv[1]), ClusterFactory())
    reactor.run()
    f_in = open(sys.argv[3], 'w')
    for center, index in zip(GLOBAL_FINAL_CENTERS, range(len(GLOBAL_FINAL_CENTERS))):
        if center != None:
            f_in.write(center)
            f_in.write('\n')
#        if len(cent) != 4211:
#            print index, GLOBAL_FINAL_CENTERS[index]

