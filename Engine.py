"""
Jack Dwyer
26/04/2012

Engine v0.3
- Loads all default workes

"""



import yaml
import epics
import time
import zmq
import sys
from Core import DatFile
from Core import LogLine
from Core.Logger import log
from Core import DirectoryCreator
import MySQLdb as mysql

from threading import Thread
import threading

from Workers import WorkerBufferAverage
from Workers import WorkerDB
from Workers import WorkerRollingAverageSubtraction
from Workers import WorkerStaticImage



class Engine():
    """
    Starts Buffer Average worker first so buffer average can bind to the correct port
    """
    
    def __init__(self, configuration):
        self.name = "Engine"
        
        #Get Configuration settings
        try:
            stream = file(configuration, 'r') 
        except IOError:
            log(self.name, "Unable to find configuration file (config.yaml, in current directory), exiting.")
            exit()
            
        self.config = yaml.load(stream)
        
        self.rootDirectory = self.config['RootDirectory']
        self.imageTakenPV = self.config['ImageTakenPV']
        self.userChangePV = self.config['UserChangePV']
        
        log(self.name, "Engine Started")
        
        
        #ZeroMQ setup stuff
        self.context = zmq.Context()
        
        #Current User
        self.user = ""
      
        #Default workers
        self.bufferAverage = WorkerBufferAverage.WorkerBufferAverage()
        self.staticImage = WorkerStaticImage.WorkerStaticImage()
        self.rollingAverageSubtraction = WorkerRollingAverageSubtraction.WorkerRollingAverageSubtraction()
        #self.DB = WorkerDB.WorkerDB()
        
        #Connect Up all Workers, and have them ready
        self.bufferRequest = self.context.socket(zmq.REQ)
        self.bufferRequest.connect("tcp://127.0.0.1:5000")
        log(self.name, "Connected -> BufferRequest")

        self.bufferPush = self.context.socket(zmq.PUSH)
        self.bufferPush.bind("tcp://127.0.0.1:5001")
        log(self.name, "Binded -> BufferPush")

        self.staticPush = self.context.socket(zmq.PUSH)
        self.staticPush.bind("tcp://127.0.0.1:5002")
        log(self.name, "Binded -> StaticPush")

        time.sleep(0.1)

        self.rollingPush = self.context.socket(zmq.PUSH)
        self.rollingPush.bind("tcp://127.0.0.1:5003")
        log(self.name, "Binded -> RollingPush")

        time.sleep(0.1)
        
        bufferThread = Thread(target=self.bufferAverage.connect, args=(5001, 5000))
        bufferThread.setDaemon(True)
        bufferThread.start()  
            
        staticImageThread = Thread(target=self.staticImage.connect, args=(5002,))
        staticImageThread.setDaemon(True)
        staticImageThread.start()
        
        rollingAverageThread = Thread(target=self.rollingAverageSubtraction.connect, args=(5003,))
        rollingAverageThread.setDaemon(True)

        rollingAverageThread.start()

        time.sleep(0.1)
        
        self.setRootDirectory()
        self.watchForChangeOver()
        self.watchForImage()      
        
        log(self.name, "All Workers ready")

        self.helpMenu() #Prints out command menu

        #Start this thread last
        cliThread = Thread(target=self.cli())
        cliThread.setDaemon(True)
        cliThread.start()
        
    def setRootDirectory(self):
        self.sendCommand("rootDirectory")
        self.sendCommand(self.rootDirectory)
        
        #EPICS MONITORING
    def watchForChangeOver(self):
        epics.camonitor(self.userChangePV, callback=self.setUser)
        
    def watchForImage(self):
        epics.camonitor(self.imageTakenPV, callback=self.imageTaken)
        
    def imageTaken(self, **kw):
        log(self.name, "image taken")
        #TEST IMAGE TYPE
        self.sendImage()
        
    def getUser(self, path):
        """Splits file path, and returns only user"""
        user = path.split("/")
        user = filter(None, user) #needed to remove the none characters from the array
        return user[-1] #currently the user_epn is the last object in the list
    
    def setUser(self, char_value = False, **kw):
        #TODO remove this, need another way to pass user directly
        if not (char_value): #needed for cli, though my kill engine, if user monitor doesnt return a valid value
            char_value = raw_input("Enter User: ")
        
        self.user = self.getUser(char_value) #get new user
        log(self.name, "User Changed -> " + str(self.user))
        
        self.sendCommand("updateUser")
        self.sendCommand(self.user)
    

       
    def bufferTaken(self):
        t = raw_input("Enter a Test String (will be sent as an object) > ")
        self.sendBuffer(t)
    
    #Engine Control       
    def requestAverageBuffer(self):
        self.bufferRequest.send("reqBuffer")
        f = self.bufferRequest.recv_pyobj()
        print f
  
    def returnUser(self):
        log(self.name, "Current User : " + self.user)
        self.sendCommand("getUser")
        
    def a(self):
        self.epicImageTaken()
        
    
    
    def cli(self):
        while True:
            time.sleep(0.1) #TODO: fixing printing output. - This is a quick fix to give enough time for all threads and workers
                            #to print/log out there data
            command = str(raw_input(">> "))
            if (command == "exit"):
                self.exitEngine()
            if (command == "help"):
                self.helpMenu()
            if not hasattr(self, command):
                print "%s is not a valid command" % command
                print "Use 'help' to list all commands"
            else:
                getattr(self, command)()
        
    def helpMenu(self):
        print '%30s' % "==================== Commands ========="
        formatting  = '%30s %10s %1s'
        print formatting % ("help", '--', "Prints help menu"+"\n"),
        print formatting % ("testPush", '--', "Runs push test across workers"+"\n"),
        print formatting % ("testRequest", '--', "Runs request test against BufferAverage" +"\n"),
        print formatting % ("setUser",'--', "Set Current User from Engine"+"\n"),
        print formatting % ("returnUser",'--', "Returns Current User from Engine and all workers"+"\n"),
        print formatting % ("imageTaken", '--', "Force Image Taken Routine")
        print formatting % ("requestAverageBuffer", "--", "Request for latest average buffer")
        print formatting % ("epicSetUser", '--', "Force Epics to have a new user")
        print formatting % ("exit", '--', "Exit Engine")


        
    def exitEngine(self):
        self.sendCommand("exit")
        time.sleep(0.2)
        log(self.name, "Exiting")
        sys.exit()

        
        
    #########
    # Helper functions
    def sendCommand(self, command):
        self.staticPush.send(command)
        self.bufferPush.send(command)
        self.rollingPush.send(command)
        
    def sendImage(self):
        self.staticPush.send("static_image")
        self.rollingPush.send("static_image")

    def sendBuffer(self, datFile):
        self.bufferPush.send("buffer")
        self.bufferPush.send_pyobj(self.datFile)
        
        
    #For Testing    
    def testPush(self):
        command = raw_input("Enter Test String (to be pushed) > ")
        self.sendCommand("testPush")
        self.sendCommand(command)
        
    def testRequest(self):
        self.bufferRequest.send("test")
        test = self.bufferRequest.recv_pyobj()
        log(self.name, "RESPONSE RECIEVED -> " + test)
        
    def epicSetUser(self):
        user = raw_input("Enter new user > ")
        epics.caput("13SIM1:TIFF1:FilePath", "/some/where/on/the/" + user + bytearray("\0x00"*256))
        
    def epicImageTaken(self):
        epics.caput("13SIM1:cam1:NumImages.VAL", 1, wait=True)

        

        

if __name__ == "__main__":
    engine = Engine("config.yaml")
    engine.testPush()
    engine.testRequest()
    engine.watchForChangeOver()