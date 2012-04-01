#!/usr/bin/env python2.7
"""
Jack dywer
23 march 2012

Averages the Static Image, and subtracts the averaged Buffer Average
"""

import zmq
from CommonLib import Logger
from CommonLib import DatFile
from CommonLib import DatFileWriter
from CommonLib import AverageList

import sys
import time
from threading import Thread


class WorkerRollingAverageSubtraction():

    def __init__(self):
        self.name = "WorkerRollingAverageSubtraction" # For logging
        Logger.log(self.name, "Worker Generated")
        
        self.allIntensities = []
        self.allQ = []
        self.allErrors = []
        
        self.aveIntensities = []
        self.aveQ = []
        self.aveErrors = []
               
        self.subtractedIntensities = []
        
        self.ave = AverageList.AverageList()
        self.datWriter = DatFileWriter.DatFileWriter()
        
        self.firstTime = True
        
    def clear(self):
        """Function for clearing data from the worker, for the next
        experiment"""
        self.allIntensities = []
        self.allQ = []
        self.allErrors = []
        
        self.aveIntensities = []
        self.aveQ = []
        self.aveErrors = []
               
        self.subtractedIntensities = []
        
        self.firstTime = True

        Logger.log(self.name, "Worker Cleared - forgotten all previous buffers and datfiles")


    
    def run(self, datFile, aveBuffer):
        self.allIntensities.append(datFile.intensities)
        self.allQ.append(datFile.q)
        self.allErrors.append(datFile.errors)

        #averaging out
        self.aveIntensities = self.ave.average(self.allIntensities)
        self.aveQ = self.ave.average(self.allQ)
        self.aveErrors = self.ave.average(self.allErrors)


        Logger.log(self.name, "Averaging Completed")
        
        self.subtract(aveBuffer)
        
        self.datWriter.writeFile("Sim/testWriter/", "newTEST.dat", { 'q': self.aveQ, 'i' : self.aveIntensities, 'errors':self.aveErrors})
        
        

        
               
        
    def subtract(self, buffer):
        subtractedIntensities = []
        for i in range(len(self.aveIntensities)):
            value = (self.aveIntensities[i] - buffer[i])
            subtractedIntensities.insert(i, value)
        
        self.subtractedIntensities = subtractedIntensities
        
        Logger.log(self.name, "Subtraction Completed")
     


if __name__ == "__main__":
    worker = WorkerRollingAverageSubtraction()

    if len(sys.argv) > 1 and sys.argv[1] == "tests":
        buf = [1,1,1]
        dat = DatFile.DatFile("Sim/data/test.dat")
        worker.run(dat, buf)

    else:    

        context = zmq.Context()

        #TODO, possible sub/pub so only one socket for all samples
        samples = context.socket(zmq.PULL)
        samples.connect("tcp://127.0.0.1:7885")

        bufferReq = context.socket(zmq.REQ)
        bufferReq.connect("tcp://127.0.0.1:7883")


        aveBuffer = []

        while True:
            filter = samples.recv()
            if (str(filter) == "sample"): 
                datFile = samples.recv_pyobj()
                Logger.log(worker.name, "Recieved DatFile")
                if (worker.firstTime):
                    bufferReq.send("REQ-AVEBUFFER")
                    aveBuffer = bufferReq.recv_pyobj()
                    worker.firstTime = False
                worker.run(datFile, aveBuffer)
            
            if (str(filter) == "clear"):
                worker.clear()

