""""
Jack DWyer
May 4 2012
SAXS - WAXS simulator/test harness for Auto Processor Engine
"""
import os
import sys
import time
import epics
sys.path.append("../")


from Core.Logger import log as log
import yaml
from threading import Thread
from Core import LogLine


class Sim:
    

    def __init__(self, configFile):
        self.name = "Simulator"
        #Load simulation configurationfile
        try:
            stream = file(configFile, 'r') 
        except IOError:
            log(self.name, "Unable to find configuration, exiting.")
            exit()
            
        self.config = yaml.load(stream)
        self.datFileLocation = self.config["datFileLocation"]
        self.absoluteLocation = self.config["absoluteLocation"]
        self.logFileLocation = self.config["logFileLocation"]
        self.imageChangePV = self.config["imageChangePV"]
        self.userChangePV = self.config["userChangePV"]
        self.user = ""
        self.relative = self.user + "/raw_dat/"
        self.fullPath = ""
        
        self.setUser()
        
        self.setImageLocationEpics()
    
        
        
    def setRelative(self):
        self.relative = self.user + "/raw_dat/"
    
    def setFullPath(self):
        self.fullPath = self.absoluteLocation + self.user


    def pause(self):
        log(self.name, "Function for pausing simulator")
            
    def start(self):
        log(self.name, "starting simulator")
        
    def setUser(self):
        #For setting current EPN user
        self.user = raw_input('Enter User: ')
        self.setRelative()
        self.setFullPath()
        
    def setImageLocationEpics(self):
        epics.caput("13SIM1:TIFF1:FilePath", self.fullPath + bytearray("\0x00"*256))


            
    def generateLog(self):
        log = open(self.absoluteLocation + self.relative + "livelog.log", "w")
        log.close()

if __name__ == "__main__":
    sim = Sim("simconfig.yaml")
    #simThread = Thread(target=Sim, args=("simconfig.yaml",))
    #simThread.start()
    

    
    
    