import logging
import sys
sys.path.append("../")

try:
    import zmq
    import MySQLdb as mysql
    import yaml
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    # sys.exit(2) #this line terminated sphinx docs building on readthedocs.
import time
from threading import Thread
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
    | Utilises the PUB-SUB design pattern from ZMQ
    """
      
    def __init__(self, config, **kwargs):
        Worker.__init__(self, "WorkerDB")
        
        self.db = None
        self.config = config
        
        #Specific ZMQ stuff for WorkerDB, it uses SUB/PUB
        self.sub = self.context.socket(zmq.SUB)

        
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
        
   
    def connect(self, pullPort = False, subPort = False):
        """
        Over ridden connect funciton to support the subPort and sub type connection
        
        Raises:
            Exception: ZMQ-Error if unable to connect
        """
        try:
            if (pullPort):
                self.pull.bind("tcp://127.0.0.1:"+str(pullPort))

            if (subPort):
                self.sub.bind("tcp://127.0.0.1:"+str(subPort))
                self.sub.setsockopt(zmq.SUBSCRIBE, "")
                
                subThread = Thread(target=self.subscribe)
                subThread.setDaemon(True)
                subThread.start()
                
            self.logger.info("Connected Pull Port at: %(pullPort)s - Subscribed Port at: %(pubPort)s" % {'pullPort' : pullPort, 'pubPort' : subPort})
        
        except:  
            self.logger.critical("ZMQ Error - Unable to connect")
            raise Exception("ZMQ Error - Unable to connect")
        
        self.run()
    
    
    def subscribe(self):
        """
        Function ran inside a thread, WorkerDB waits and responds to any published requests. for controlling what is written to the db and where
        """        
        try:
            while True:
                
                recievedObject = self.sub.recv_pyobj()
                self.logger.info("Received Object")
                try:
                    command = str(recievedObject['command'])
                except KeyError:
                    self.logger.error("No command key sent with object, can not process request")
                    continue

                if (command == "averaged_buffer"):
                    self.logger.info("Written location of averaged buffer")
                    self.writeBufferLocation(recievedObject["location"])
                    continue
                
                if (command == "averaged_subtracted_sample"):
                    self.logger.info("Written location of averaged_subtracted_sample")
                    self.writeAveragedSubtactedLocation(recievedObject["location"])
                    continue
                
                if (command == "averaged_sample"):
                    self.logger.info("Written location of averaged_sample")
                    self.writeAveragedLocation(recievedObject["location"])
                    continue
                
                if (command == "subtracted_sample"):
                    self.logger.info("Written location of subtracted_sample")
                    self.writeSubtractionLocation(recievedObject["location"])
                    continue
                
                if (command == "test"):
                    self.logger.info("Gotten TEST COMMMAND")
                    continue
        except KeyboardInterrupt:
                pass
        
        self.close()
        
    def close(self):
        """
        Method for properly shutting down worker, includes the sub port
        
        Raises:
            Exception: if it failed fo close port
        """
        try:
            time.sleep(0.1)
            self.sub.close()
            self.logger.info("Closed ports - shutting down")
        except:
            self.logger.critical("Failed to close ports")
            raise Exception("Failed to close ports")
        finally:
            sys.exit()
            
    
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
    context = zmq.Context()
    port = 1211
    
    with open("../settings.conf", 'r') as config_file:
        config = yaml.load(config_file)
            
    worker = WorkerDB(config)
    print worker.getName()

    t = Thread(target=worker.connect, args=(port,))
    t.start()
    time.sleep(0.1)

    testPub = context.socket(zmq.PUB)
    testPub.connect("tcp://127.0.0.1:"+str(port))

    testPub.send_pyobj({'command' : "test"})
    testPub.send_pyobj({'command' : "update_user", "user":"testuser"})
    testPub.send_pyobj({'command' : "createDB"})
    testPub.send_pyobj({'command' : "createDB", "database":"testdb"})
    testPub.send_pyobj({'command' : "shut_down"})
