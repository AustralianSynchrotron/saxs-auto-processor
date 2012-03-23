#!/usr/bin/env python2.7
"""
Jack Dwyer
18 March 2012

Engine for the SAXS/WAXS Auto Processor
"""

import epics
import time
import zmq
from CommonLib import LogLine
from CommonLib import TableBuilder
from CommonLib import DatFile
import MySQLdb as mysql


class Engine():
    def __init__(self):
        #To make sure we dont miss any loglines
        self.index = 0
        
        #User setup
        self.user = ""
        self.experiment = "EXPERIMENT_1"
        self.logFile = ""
        
        #File Locations
        self.logLocation = "testDat/livelogfile.log"
        self.datFileLocation = "/home/jack/sim/"
        
        #ZeroMQ setup stuff
        self.context = zmq.Context()
        self.bufferWorker = self.context.socket(zmq.PUSH)
        self.bufferWorker.bind("tcp://*:7881")  
        self.sampleWorker = self.context.socket(zmq.PUSH)
        self.sampleWorker.bind("tcp://*:7882")
        #7883 is used by the WorkerBufferAverage
        self.dbWorker = self.context.socket(zmq.PUSH)
        self.dbWorker.connect("tcp://127.0.0.1:7884")
    
        #For holding the data
        self.lines = [] #List of lines already read
        self.logLines = [] #List of LogLine Objects, that have been broken down for easy access
        self.latestLine = ""
        self.datFiles = []
        
        #For serialisation of objects/data
        
        #Make sure all sockets are created
        time.sleep(1.0)
    
 
        
    def readLatestLine(self):
        noLines = True
        while (noLines):          
            try:
                print "trying to open file"
                v = open(self.logFile, "r")
                try:
                    self.latestLine = v.readlines()[self.index]
                    if (self.latestLine in self.lines):
                        time.sleep(0.05)
                    else:
                        self.lines.append(self.latestLine)
                        self.logLines.append(LogLine.LogLine(self.latestLine))
                        self.index = self.index + 1
                        
                        #Send logline to be added to DB
                        self.dbWorker.send("logLine")
                        self.dbWorker.send_pyobj(self.logLines[self.index-1])
                        
                        noLines = False
                except IndexError:
                    pass
                                
                v.close()
            except IOError:
                print "IOERROR"
                time.sleep(0.5)
                pass
            
        
    def getDatFile(self):
        """TODO: make better... forgot the better and faster way I had the imagename
        """
        ##returns dat file location
        noDatFile = True
        #getting actual dat file name from the log line. It will only pick up that dat file
        dat = self.logLines[self.index-1].getValue("ImageLocation")
        #this needs to be fixed to os agnostic
        dat = dat.split("/")
        dat = dat[-1]
        dat = dat.split(".")
        dat = dat[0] + ".dat"
        dat = str(dat)
        while (noDatFile):
            try:
                datFile = ('testDat/'+ dat)  
                self.datFiles.append(DatFile.DatFile(datFile))               
                noDatFile = False
            except IOError:
                time.sleep(0.05)
    
    
    
    
    
    def epicPVChange(self, value, **kw ):
        """Check Logline, get all details on latest image """
        print "Value Changed"
        
        if value == 100:
            print "Value = 100"
            self.readLatestLine()
            print "read last line"
            self.getDatFile()
            print "got datfile"
            print self.datFiles[self.index-1].getDatFilePath()
            
            
            imageType = (self.logLines[self.index-1].getValue("SMPL_TYPE"))
            
            print imageType
            
            #if (imageType == "BUFFER"):
            if (imageType == "BUFFER"):
                #Sent datFile object to worker
                self.bufferWorker.send_pyobj(self.datFiles[self.index-1])
            if (imageType == "STATIC_SAMPLE"):
                self.sampleWorker.send_pyobj(self.datFiles[self.index-1])


        
        
    




    def run(self, user):
        self.user = user
        self.dbWorker.send("user")
        self.dbWorker.send(str(self.user))  
        self.dbWorker.send("Experiment")
        self.dbWorker.send(str(self.experiment))      
        
        #Setup Variables/File Locations for user
        self.logFile = "testDat/livelogfile.log"
                       
        epics.camonitor("13SIM1:cam1:NumImages_RBV", callback=self.epicPVChange)
        
        try:
            while True:
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass



if __name__ == "__main__":
    engine = Engine()
    #Should do check for changeover script here
    user = raw_input("ENTER USER >> ")
    engine.run(user)