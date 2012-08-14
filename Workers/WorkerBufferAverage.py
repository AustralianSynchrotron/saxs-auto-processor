import logging
import sys
try:
    import zmq
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    # sys.exit(2) #this line terminated sphinx docs building on readthedocs.
import time
from threading import Thread
from Worker import Worker
from Core import AverageList
from Core import DatFile
from Core import DatFileWriter



class WorkerBufferAverage(Worker):  
    """
    | Average Buffer Worker
    | Takes a constant steam of buffers for a related sample and averages it out
    | Utilises the REQ-REP design pattern from ZMQ
    """
      
    def __init__(self, **kwargs):
        Worker.__init__(self, "Worker Buffer Average")
        
        #Specific Class Variables
        self.averagedBuffer = None
        self.bufferIndex = 1
        self.buffers = []
        self.averagedIntensities = None
        
        self.previousName = None
        
        
        #specific ZMQ 
        self.context = zmq.Context()
        self.reply = self.context.socket(zmq.REP)

    
    def connect(self, pullPort = False, pubPort = False, replyPort = False):
        """
        Over ridden Connect Function, adds in the replyport so the Engine can contact this worker for the latest Averaged Buffer
        """
        try:
            if (pullPort):
                self.pull.bind("tcp://127.0.0.1:"+str(pullPort))

            if (pubPort):
                self.pub.connect("tcp://127.0.0.1:"+str(pubPort))
            
            if (replyPort):
                self.reply.bind("tcp://127.0.0.1:"+str(replyPort))
                
                replyThread = Thread(target=self.requestBufferThread)
                replyThread.setDaemon(True)
                replyThread.start()
                
                
            self.logger.info("Connected Pull Port at: %(pullPort)s - Publish Port at: %(pubPort)s - Reply Port at: %(replyPort)s" % {'pullPort' : pullPort, 'pubPort' : pubPort, 'replyPort':replyPort})
        
        except:  
            self.logger.critical("ZMQ Error - Unable to connect")
            raise Exception("ZMQ Error - Unable to connect")
        
        self.run()

    
        
    def processRequest(self, command, obj):    
        """
        Handles any commands passed to the worker if the generic run() method is unable to find a match
        """            
        self.logger.info("Processing Received object")
        command = str(obj['command'])

        if (command == "new_buffer"):
           self.newBuffer()

        if (command == "buffer"):
            buffer = obj['buffer']
            self.logger.info("Buffer Sample")
            self.averageBuffer(buffer)
            
            
            
    def averageBuffer(self, buffer):
        """
        Function for averaging out and writing out the averaged buffer
        """
        self.buffers.append(buffer)
        intensities = []
        for buffer in self.buffers:
            intensities.append(buffer.getIntensities())
            
        datName = "avg_buffer_" + str(self.bufferIndex) + "_" +buffer.getBaseFileName()
        self.averagedIntensities = self.averageList.average(intensities)
        
        self.datWriter.writeFile(self.absoluteLocation + "/avg/", datName, { 'q' : self.buffers[-1].getq(), "i" : self.averagedIntensities, 'errors':self.buffers[-1].getErrors()})
        
        if not (self.previousName == datName):
            self.pub.send_pyobj({"command":"averaged_buffer", "location":datName})
            self.previousName = datName



    
    def requestBufferThread(self):
        """
        This is a function placed in its own thread, allows the Engine to communicate and ask for the current Averaged Buffer and wait for a Response

        """
        
        try:
            while True:
                test = self.reply.recv() #wait for request of buffer
                if (test == 'test'):
                    self.reply.send_pyobj("REQUESTED DATA")
                if (test == "request_buffer"):
                    if (self.averagedIntensities):
                        self.reply.send_pyobj(self.averagedIntensities)      
                    else:
                        self.reply.send_pyobj(False)
        except KeyboardInterrupt:
            pass   




    def rootNameChange(self):
        self.logger.info("Root Name Change Called - No Action Required")

    def newBuffer(self):
        """
        | Clears out the current buffer as expecting new one.
        | Increments sample number
        """
        self.buffers = []
        self.averagedBuffer = None
        self.bufferIndex = self.bufferIndex + 1
    
    def clear(self):
        """
        Calls super clear() and then the workers specific requirements
        """
        Worker.clear(self)
        self.averagedBuffer = None
        self.bufferIndex = 1
        self.buffers = []

        

if __name__ == "__main__":
    #Test Cases
    context = zmq.Context()
    port = 1200
    
    worker = WorkerBufferAverage()

    t = Thread(target=worker.connect, args=(port,))
    t.start()
    time.sleep(0.1)

    
    testPush = context.socket(zmq.PUSH)
    testPush.connect("tcp://127.0.0.1:"+str(port))
    testPush.send_pyobj({'command' : "averaged_buffer"})
    testPush.send_pyobj({'command' : "test"})
    testPush.send_pyobj({'command' : "test"})
    testPush.send_pyobj({'command' : "static_image"})
    testPush.send_pyobj({'command' : "test"})
    testPush.send_pyobj({'command' : "test"})
    testPush.send_pyobj({'command' : "averaged_buffer"})
    testPush.send_pyobj({'command' : "test"})
    testPush.send_pyobj({'command' : "test"})
    testPush.send_pyobj({'command' : "static_image"})
    testPush.send_pyobj({'command' : "test"})
    testPush.send_pyobj({'command' : "averaged_buffer"})
    testPush.send_pyobj({'command' : "shut_down"})
