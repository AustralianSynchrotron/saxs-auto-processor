import sys
try:
    import zmq
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    # sys.exit(2) #this line terminated sphinx docs building on readthedocs.
sys.path.append("../")
import time
import logging
from threading import Thread
from Core import AverageList
from Core import DatFile
from Core import DatFileWriter
from Core import OutLierRejection


class Worker():
    """
    The base class for all Workers to inherit from, has some generic methods
    """
    
    def __init__(self, name = "Default Worker"):
        #General Variables
        self.name = self.__class__.__name__
        
        
        #ZMQ stuff
        self.context = zmq.Context()
        self.pull = self.context.socket(zmq.PULL)
        self.pub = self.context.socket(zmq.PUB)
        
        #Class objects
        self.datWriter = DatFileWriter.DatFileWriter()
        self.averageList = AverageList.AverageList()
        self.rejection = OutLierRejection.OutLierRejection()
        
        #Class Variables
        self.logger = None
        
        #User Specific Variables (eg, user, their location relative to engine)
        self.user = None
        self.absoluteLocation = None
        
        
        
        #Setup logging 
        self.setLoggingDetails()
        


        
    def connect(self, pullPort = False, pubPort = False):
        """
        Connects worker to ZMQ ports
         
        Args:
            PullPort: The pull side of the connection, the worker will always bind this
            PubPort: The publish port, for publishing to the WorkerDB, always will be connect except at WorkerDB end
        
        Puts worker in run() where it will keep alive recieving commands
        
        Raises:
            ZMQ-Error: if unable to connect
        """
        
        try:
            if (pullPort):
                self.pull.bind("tcp://127.0.0.1:"+str(pullPort))

            if (pubPort):
                self.pub.connect("tcp://127.0.0.1:"+str(pubPort))
                
            self.logger.info("Connected Pull Port at: %(pullPort)s - Publish Port at: %(pubPort)s" % {'pullPort' : pullPort, 'pubPort' : pubPort})
        
        except:  
            self.logger.critical("ZMQ Error - Unable to connect")
            raise Exception("ZMQ Error - Unable to connect")
        
        self.run()


        
    
    def run(self):
        """
        Is a while loop that parses the commands sent to it from the pull port, if no command is found after parses sends command object to processRequest
        
        Contains all the default commands and functions expected in every worker
        
        """
        
        try:
            while True:
                receivedObject = self.pull.recv_pyobj()
                self.logger.info("Received Object")
                if (self.runCommand(receivedObject) == False):
                    break
                               
        except KeyboardInterrupt:
                pass
        
        self.logger.info("Shutting Down")
        self.close()
    
    def runCommand(self, receivedObject):
        
        try:
            command = str(receivedObject['command'])
        except KeyError:
            self.logger.error("No command key sent with object, can not process request")
            return
        
        #Default Commands            
        if (command == "update_user"):
            self.clear()
            try:
                self.setUser(receivedObject['user'])
            except KeyError:
                self.logger.error("Malformed command dictionary")
            return
            
        if (command == "absolute_directory"):
            self.setDirectory(receivedObject['absolute_directory'])
            return
        
        if (command == "root_name_change"):
            self.rootNameChange()
            return
        
        if (command == "new_buffer"):
            self.newBuffer()
            return
        
        if (command == "clear"):
            self.clear()
            return
           
        if (command == "shut_down"):
            return False
       
        #Test commands
        if (command == "test"):
            self.logger.info("test command received")
            return
         
        if (command == "test_receive"):
            print receivedObject['test_receive']
            return
        
        if (command == "get_variables"):
            print self.user
            print self.absoluteLocation
            print self.getName()
            
        else:
            self.processRequest(command, receivedObject)      
            return
               
    
    #This must be overridden
    #It specifies how you want to process specific commands for workers   
    def processRequest(self, command, obj):
        """
        If the command passes the generic requests, it is pass down here where this function has to be overwritten to handle the specific worker needs
        
        Raises:
            Exception: if it is now over ridden
        """
        raise Exception("Must override this method")
    
    def rootNameChange(self):
        """
        Must be over ridden, so the worker can handle the root name change event
        
        Raises:
            Exception: if it is not over ridden
        """
        raise Exception("Must override this method")

    def newBuffer(self):
        """
        Must be over ridden, so the worker can handle the new buffer event
        
        Raises:
            Exception: if it is not over ridden
        """
        raise Exception("Must override this method")


 
         
    #Generic Methods shared by all workers - WorkerDB over rides some stuff
    def setUser(self, user):
        """
        Sets the workers user to the user passed by the command string
        """
        self.user = str(user)
        self.logger.info("User set to %(user)s" % {'user':self.user})
                
    def setDirectory(self, directory):
        """
        Sets the workers path to the path passed by the command string
        """
        self.logger.info("Setting Absolute Directory - %s" % directory)
        self.absoluteLocation = directory
     
    def getName(self):
        """
        Mainly used for testing to allow for all workers to reply
        """        
        return self.name
        
    #This should be called from super class    
    def clear(self):
        """
        Standard function for clearing out the Workers after a new user or experiment as occured
        """
        self.logger.info("Cleared Details")
        self.user = None
        self.absoluteLocation = None 
        
    def close(self):
        """
        Close's ZMQ sockets correctly and kills workers
        
        Raises:
            Exception: if it fails
        
        Finally:
            Kills teh worker (sys.exit())
        """
        try:
            time.sleep(0.1)
            self.pull.close()  
            self.logger.info("Closed ports")
        except:
            self.logger.critical("Failed to close ports")
            raise Exception("Failed to close ports")
        finally:
            sys.exit()
    
    #Generic method to publish data to the database worker
    def pubData(self, command):
        """
        Generic Function for publishing data
        """
        self.pub.send({'command':command})
    
    
    
    
    
    ###Logging Setup
    def setLoggingDetails(self):
        """
        Generic Logging Details
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
    #Test Cases
    worker = Worker()

    print "sent command"

    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "test"})
    
    worker.runCommand({'command':"update_user", "user":"Tom"})


    #v = raw_input(">>")
    #worker.runCommand({'command' : "test_receive", "test_receive" : v})
    worker.runCommand({'command' : "shut_down"})
    
    
