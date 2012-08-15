import logging
import sys
sys.path.append("../")

try:
    import MySQLdb as mysql
    import yaml
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    # sys.exit(2) #this line terminated sphinx docs building on readthedocs.
from Worker import Worker
from Core import DatFile
from Core import TableBuilder

try:
    from sqlalchemy import *
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    # sys.exit(2) #this line terminated sphinx docs building on readthedocs.

class WorkerDB(Worker):  
    """
    | Worker for controlling CRUD of database
    | Uses SQLAlchemy Library
    """
      
    def __init__(self, config, **kwargs):
        Worker.__init__(self, "WorkerDB")
        
        self.db = None
        self.config = config
        
    def processRequest(self, command, obj):                
        self.logger.info("Processing Received object")
        command = str(obj['command'])
        if (command == "log_line"):
            self.writeLogToDB(obj['line'])
        
        if (command == "createDB"):
            try:
                self.createDB(obj['database'])
            except KeyError:
                self.createDB(self.user)
        
    def send(self, receivedObject):
            self.handleSubscriber(receivedObject)
            
    def send_pyobj(self, receivedObject):
            self.handleSubscriber(receivedObject)
            
    def handleSubscriber(self, receivedObject):
        
        try:
            command = str(receivedObject['command'])
            self.logger.info("Received command %s into WorkerDB." % (command,))
        except KeyError:
            self.logger.error("No command key sent with object, can not process request")
            return
        except TypeError:
            self.logger.error("Command object not a dictionary, can not process request")
            return
        if (command == "averaged_buffer"):
            self.logger.info("Written location of averaged buffer")
            self.writeBufferLocation(receivedObject["location"])
            return
        
        if (command == "averaged_subtracted_sample"):
            self.logger.info("Written location of averaged_subtracted_sample")
            self.writeAveragedSubtactedLocation(receivedObject["location"])
            return
        
        if (command == "averaged_sample"):
            self.logger.info("Written location of averaged_sample")
            self.writeAveragedLocation(receivedObject["location"])
            return
        
        if (command == "subtracted_sample"):
            self.logger.info("Written location of subtracted_sample")
            self.writeSubtractionLocation(receivedObject["location"])
            return
        
        if (command == "test"):
            self.logger.info("Gotten TEST COMMMAND")
            return

    
    def rootNameChange(self):
        pass
    
    def newBuffer(self):
        pass
    
    
    def createDB(self, dbname):
        """
        Force creates a database in the local mysql
        """
        if not dbname:
            self.logger.critical("Empty dbname!")
            return
        self.logger.info("Creating database: '%(dbname)s'" % {'dbname':dbname})
        database = self.config.get('database')
            
        try:
            db = mysql.connect(user=database['user'], host=database['host'], passwd=database['passwd'])
            c = db.cursor()
            cmd = "CREATE DATABASE IF NOT EXISTS %s;" % (dbname, )
            c.execute(cmd)      
        except Exception:
            raise
        
        self.buildTables(dbname)
        
    def buildTables(self, dbname):
        """
        Uses SQL Alchemy to create logtable objects that build the tables inside the database and then allow for data to be written against it
        """
        collumAttributes = ['WashType', 'SampleOmega', 'FilePluginDestination', 'Temperature2', 'Temperature1', 'WellNumber', 'SamplePhi', 'NumericTimeStamp', 'I0', 'SampleY', 'SampleX', 'SampleChi', 'TimeStamp', 'SampleType', 'ImageCounter', 'Ibs', 'exptime', 'FilePluginFileName', 'Energy', 'It', 'SampleTableX', 'SampleTableY', 'NORD', 'ImageLocation']

        #collumAttributes_old = ['I0', 'NumericTimeStamp', 'WashType', 'FilePluginDestination', 'TimeStamp', 'Energy', 'NORD', 'SampleType', 'It', 'SampleTableX', 'SampleTableY', 'Temperature2', 'Temperature1', 'WellNumber', 'Ibs', 'exptime', 'FilePluginFileName', 'ImageLocation']
        self.logTable = TableBuilder.TableBuilder(self.config, dbname, "Log", collumAttributes)
        
        #This needs to bs fixed to support different sample types
        bufferColumns = ['buffer_location']
        self.bufferTable = TableBuilder.TableBuilder(self.config, dbname, 'buffers', bufferColumns)
        
        subtractedColumns = ['subtracted_location', 'avg-low-q', 'avg-high-q', 'valid']
        self.subtractedTable = TableBuilder.TableBuilder(self.config, dbname, 'subtracted_images', subtractedColumns)
        
        averagedColumns = ['average_location']
        self.averagedTable = TableBuilder.TableBuilder(self.config, dbname, 'average_images', averagedColumns)
        
        averagedSubColumns = ['average_subtracted_location', 'porod_volume']
        self.averagedSubTable = TableBuilder.TableBuilder(self.config, dbname, 'average_subtracted_images', averagedSubColumns)
        
        damVolColumns = ['dammif_pdb_file', 'dam_volume', 'average_subtracted_images_fk']
        self.damVolTable = TableBuilder.TableBuilder(self.config, dbname, 'dam_volumes', damVolColumns)

    
    
    def writeLogToDB(self, logLine):
        """
        Writes out log line to the database
        """
        self.logTable.addData(logLine.data)
        
    def writeSubtractionLocation(self, image):
        """
        For sending the subtracted files name (thusly lcoation to the database)
        """
        self.subtractedTable.addData({ "subtracted_location" : image })
    
    def writeBufferLocation(self, image): 
        """
        For sending the averaged buffer files name (thusly lcoation to the database)
        """
        self.bufferTable.addData({ "buffer_location" : image })
        
    def writeAveragedLocation(self, image):
        """
        For sending the averaged files name (thusly lcoation to the database)
        """
        self.averagedTable.addData({ "average_location" : image })
    
    def writeAveragedSubtactedLocation(self, image):
        """
        For sending the averaged and subtracted files name (thusly lcoation to the database)
        """
        self.averagedSubTable.addData({ "average_subtracted_location" : image })
        
        
    
            
            
if __name__ == "__main__":
    #Test Cases
   
    with open("../settings.conf", 'r') as config_file:
        config = yaml.load(config_file)
            
    worker = WorkerDB(config)
    print worker.getName()

    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "update_user", "user":"testuser"})
    worker.runCommand({'command' : "createDB"})
    worker.runCommand({'command' : "createDB", "database":"testdb"})
    worker.runCommand({'command' : "shut_down"})
