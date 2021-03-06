#!/usr/bin/python

###BASH start stop
#screen -dmS "engine" "./engine.py"


import logging
import sys
import time
import os
import traceback

try:
    import epics
    import yaml
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    #sys.exit(2) #this line terminated sphinx docs building on readthedocs.

from threading import Thread

from Core.EngineFunctions import getString as getString
from Core.EngineFunctions import testStringChange as testStringChange
from Core.EngineFunctions import createFolderStructure as createFolderStructure
from Core.RootName import changeInRootName

from Core import LogWatcher
from Core import LogLine
from Core import DatFile


class Engine():
    """
    Is the goto man for controlling the sequence of events that will occur after a datFile has been created
    
    """
    def __init__(self, configuration):
        self.name = "Engine"

        #Read all configuration settings
        self.config = None
        self.workers = None   
        self.rootDirectory = None             

        try:
            self.setConfiguration(configuration)
        except IOError:
            print "Unable to read configuration file from %s, exiting." % (configuration, )
            sys.exit(1)
        
        # setup logging                    
        self.logger = None
        self.setLoggingDetails()
        
        #Instantiate class variables
        self.first = True #For catching index error
        self.user = None
        self.logLines = []
        self.needBuffer = True
        
        # Object that will be watching the LiveLogFile
        self.logWatcher = LogWatcher.LogWatcher()
        
        #SET Correctly in newUser
        self.liveLog = None
        self.datFileLocation = None
        self.previousUser = None #Used to compare current user against new user (to stop if users click the pv etc)
        self.previousExperiment = None

        #Instantiate all workers, get them all ready to push out into their own thread and connected up
        self.instanceWorkerDict = self.instantiateWorkers(self.workers)
        #Connect up workers
        self.connectWorkersToDb()

    def setConfiguration(self, configuration):
        """Reads the default configuration file that is passed at object creation 
        The configuration stores the Workers that need to be loaded, whether an Experiment name is being used, this allows for any workers
        to be loaded dynamically at run time.        
        The Absolute location of the datFiles.
        Any PV's that need to be watched
    
        Args:
            Configuration (file): A YAML config file
          
        Returns:
           Nothing
           
        Sets class Variables:
            | self.rootDirectory = The absolute location of the experiments as mounted on the local machine.
            | self.userChangePV = The FullPath PV from epics to watch for user/experiment change over.
            | self.experimentFolderOn = Switch if they experiment folders are being used.
            | self.workrs = List of all workers that need to be instantiated.
          
        Raises:
           IOError: When it is unable to find the configuration
        """    
        with open(configuration, 'r') as config_file:
            self.config = yaml.load(config_file)

            self.rootDirectory = self.config.get('RootDirectory')
            self.userChangePV = self.config.get('UserChangePV')
            self.experimentFolderOn = self.config.get('ExperimentFolderOn')                
            self.workers = self.config.get('workers')
        
    def instantiateWorkers(self, workers):
        """Instantiates each worker as specified by the Configuration 
        
        Args:
            Workers: A list of string names of each worker
            
        Returns:
            instanceDict: A dictionary of Key (Worker Name String): Value (Instantiated Worker Object)
        """
        self.logger.info("Instantiating all workers")
        instanceDict = {}
        for worker in workers:
            im = __import__('Workers.'+worker, globals(), locals(), [worker])
            v = getattr(im, worker)
            x = v(config=self.config)
            instanceDict[worker] = x
        return instanceDict


    def connectWorkersToDb(self):
        for workerName, worker in self.instanceWorkerDict.items():
            worker.pub = self.instanceWorkerDict['WorkerDB']

    # Event Watching
    def setUserWatcher(self):
        """
        Sets up a epics.camonitor against the PV set by the configuration file
        
        Callback:
            setUser()
        """
        
        epics.PV(self.userChangePV, callback=self.setUser)
        
    def watchForLogLines(self, logLocation):
        """
        Creates an object for watching the logfile that callsback when ever a new line has been written
        
        Callback:
            lineCreated()
        
        """              
        self.logWatcher.kill()
        self.logWatcher.setLocation(logLocation)
        self.logWatcher.setCallback(self.lineCreated)
        self.logWatcher.watch()
        
       
    def setUser(self, char_value, **kw):
        """
        | Sets the User for the Engine, and all workers.
        | Is called when the PV changes
        | Checks new user value against previous user
        | If matching values, nothing occurs
        | Calls newUser(user) if it is a new user
        
        Args:
            char_value (string): String value of the PV, should be the full path to the image locations relative to epics
            \*\*kw (dict): remaining values returned from epics
            
        """
        
        #user = getUser(char_value)
        
        if self.experimentFolderOn:
            experimentFolder = getString(char_value, -2)
            user = getString(char_value, -3)
        else:
            experimentFolder = ""
            user = getString(char_value, -2)

        if self.previousUser != user or self.previousExperiment != experimentFolder:
            self.logger.info("Experiment Change Event\n----------------------------------------")
            self.previousUser = user; self.previousExperiment = experimentFolder
            self.setupExperiment(user, experimentFolder)

        
    def setupExperiment(self, user, experimentFolder = ""):
        """
        New User or Experiment has been found, need to communicate to myself and all workers the new details
        A new Database is created
        And the engine commences watching the logfile.
        
        Args:
            user (string): string value of the user
         
        """
                
        self.logger.info("Setting up new experiment")
        #Reset class variables for controlling logic and data
        self.first = True
        self.logLines = []
        self.needBuffer = True
        
        absolute_directory = os.path.join(self.rootDirectory, user, experimentFolder)
        self.liveLog = os.path.join(absolute_directory, "images", "livelogfile.log")
        self.datFileLocation = os.path.join(absolute_directory, "raw_dat")
        
        #Generate Directory Structure
        createFolderStructure(absolute_directory)
        self.logger.info("Directory Structure Created")
        
        #Update all workers
        self.sendCommand({"command":"update_user", "user":user})
        self.sendCommand({"command":"absolute_directory","absolute_directory":absolute_directory})
        
        
        dbname = "_".join(filter(None, [user, experimentFolder]))
        self.createDB(dbname)
        
        #Start waiting for log to appear
        self.watchForLogLines(self.liveLog) # Start waiting for the Log


    def run(self):
        """
        Starts the epics watcher for user change
        Keeps on running as the main thread
        """ 
        self.setUserWatcher() #Start epics call back
              
        while True:
            #Keep the script running
            time.sleep(0.1)
        
 
 
 
 
    def lineCreated(self, line, **kw):
        """
        | Here we parse the logline for the Image Location
        | it Preliminarily checks against the image type for weather it needs to bother looking for it or not 
        | Calls processDat if we care for the datFile
        | sends the logline to be written out to the database
        
        Args:
            line (string): returned latest line from call back
            \*\*kw (dictionary): any more remaining values
        """
        try:
            latestLine = LogLine.LogLine(line)
            self.logLines.append(latestLine)

            #Send off line to be written to db
            self.sendLogLine(latestLine)

            if (latestLine.getValue("SampleType") == "0" or latestLine.getValue("SampleType") == "1"):
                datFile = self.getDatFile(latestLine.getValue("ImageLocation"))
                if (datFile):
                    self.processDat(latestLine, datFile)
            else:
                self.logger.info("Hey, it's a sample type I just don't care for!")
        except Exception, e:
            self.logger.error("Failed to process log line: %s" % (e, ))
            traceback.print_exc()

   
   
    def getDatFile(self, fullPath):
        """
        | Called from lineCreated, is called if we want the datFile from the log line
        | It looks in the location created from the configuration file for the corresponding datFile
        | Times out after 3seconds and passes
        
        Args:
            fullPath (String): Absolute location of the datFile from the LogLine
            
        Returns:
            | datFile object created from the static image
            | or, returns False if nothing is found
        
        """
        imageName, extension = os.path.splitext(os.path.basename(fullPath))
        datFileName = os.path.join(self.datFileLocation , "%s.dat" % imageName)
        
        self.logger.info("Looking for DatFile %s" % datFileName)
   
        startTime = time.time()
        while not os.path.isfile(datFileName):
            self.logger.info("Waiting for the %s" % datFileName)
            time.sleep(0.01)
            if (time.time() - startTime > 3.0):
                self.logger.critical("DatFile: %s - could not be found - SKIPPING" % datFileName)
                return False
        
        datFile = DatFile.DatFile(datFileName)
        self.logger.info("DatFile: %s - has been found" % datFileName)
        return datFile
   
   
    def processDat(self, logLine, datFile):
        """
        
        | Here we will decide how to process the datFile
        | Sample Types:
        | 6 - Water
        | 0 - Buffer
        | 1 - Static Sample
        
        | Sample type of DatFile is determined by the logline.  We only currently care for 0 (buffer), or 1 (static Sample)
        | Is sample is a buffer, it needs to be passed to WorkerBufferAverage to be processed
        | If it is a sample it is passed to all workers to be processed by them if they want
        
        We check if the Workers need an AveragedBuffer which we then can request from WorkerBufferAverage
        We check for a rootname change indicating a new sample which may or may not require a new buffer average
        
        Args:
            logLine (LogLine Object): Latest Logline
            datFile (datFile Object): Corresponding DatFile from LogLine
            
        Raises:
            IndexError: Raised only on first pass, as we need the current user to check againse the previous user
        
        """
        
        try:
            if ((len(self.logLines) <= 1) or changeInRootName(os.path.basename(self.logLines[-1].getValue("ImageLocation")), os.path.basename(self.logLines[-2].getValue("ImageLocation")))):
                self.logger.info("There has been a change in the root name")
                
                self.sendCommand({"command":"root_name_change"})
                
                
                if (logLine.getValue("SampleType") == "0"):
                    self.logger.info("New Buffer!")
                    self.needBuffer = True
                    self.sendCommand({"command":"new_buffer"})
                    self.sendCommand({"command":"buffer", "buffer":datFile})
                
                
                if (logLine.getValue("SampleType") == "1"):
                    if (self.needBuffer):
                        averagedBuffer = self.instanceWorkerDict['WorkerBufferAverage'].getAveragedBuffer()
                        if (averagedBuffer):
                            self.sendCommand({"command":"averaged_buffer", "averaged_buffer":averagedBuffer})
                            self.needBuffer = False
                            self.sendCommand({"command":"static_image", "static_image":datFile})
                            
                        else:
                            self.sendCommand({"command":"static_image", "static_image":datFile})

                    else:
                        self.logger.info("So lets average with current buffer!")
                
            else:
                self.logger.info("No change in root name fellas")
                
                if (logLine.getValue("SampleType") == "0"):
                    self.sendCommand({"command":"buffer", "buffer":datFile})

                if (logLine.getValue("SampleType") == "1"):
                    if (self.needBuffer):
                        averagedBuffer = self.instanceWorkerDict['WorkerBufferAverage'].getAveragedBuffer()
                        if (averagedBuffer):
                            self.sendCommand({"command":"averaged_buffer", "averaged_buffer":averagedBuffer})
                            self.needBuffer = False
                            self.sendCommand({"command":"static_image", "static_image":datFile})

                        else:
                            self.logger.critical("No averaged Buffer returned unable to perform subtraction")
                    else:
                        self.sendCommand({"command":"static_image", "static_image":datFile})
                    


       
        except IndexError:
            self.logger.info("INDEX ERROR - Should never occur")
                   



    
    def cli(self):
        try:
            while True:
                print "exit - to exit"
                print "workers - sends test command out to find workers that are alive"
                print "variables - returns all the class variables of each worker"
                request = raw_input(">> ")
                if (request == "exit"):
                    self.exit()
                if (request == "workers"):
                    self.test()
                if (request == "variables"):
                    self.sendCommand({"command":"get_variables"})
                
                
                time.sleep(0.1)
        except KeyboardInterrupt:
            pass

    #Generic Methods
    def sendCommand(self, command):
        """
        Sends a structed Dictionary command to all connected Workers
        
        Args:
            command (Dictionary): requested command to be sent
        
        """
        if (type(command) is dict):
            for worker in self.instanceWorkerDict:
                self.instanceWorkerDict[worker].runCommand(command)
        else:
            self.logger.critical("Incorrect Command datatype, must send a dictionary")

    def sendLogLine(self, line):
        """
        Sends the pass logline to the WorkerDB to be written out to the database
        
        Args:
            line (LogLine object): LogLine object that you want to write to the DB
        """
        try:
            self.instanceWorkerDict['WorkerDB'].runCommand({"command":"log_line", "line":line})
        except Exception:
            self.logger.error("Failed to insert LogLine")

    def createDB(self, database):
        """
        Create the specified database for the new user
        """
        try:
            self.instanceWorkerDict['WorkerDB'].runCommand({"command":"createDB", "database":database})
        except Exception:
            self.logger.error("Failed to createDB")

    def test(self):
        self.sendCommand({'command':"test"})
        time.sleep(0.1)
        
    def exit(self):
        """
        Properly shuts down all the workers
        """
        self.sendCommand({"command":"shut_down"})
        time.sleep(0.1)
        sys.exit()
        
        
    def setLoggingDetails(self):
        """
        Current generic logging setup using the python logging module
        """
        LOG_FILENAME = 'logs/'+self.name+'.log'
        FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        logging.basicConfig(filename=LOG_FILENAME,level=logging.DEBUG,format=FORMAT)
        
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
        console.setFormatter(formatter)
        logging.getLogger(self.name).addHandler(console)
        
        self.logger = logging.getLogger(self.name)
        self.logger.info("\nLOGGING STARTED")


if __name__ == "__main__":
    eng = Engine("settings.conf")
    eng.run()



