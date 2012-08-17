import sys
sys.path.append("../")
import logging
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
        
        #Instance of workerDB - will change variable name in future.
        self.pub = None
        
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

    def runCommand(self, receivedObject):
        
        try:
            command = str(receivedObject['command'])
            #self.logger.info("Received command %s into Worker." % (command,))
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
    
    
