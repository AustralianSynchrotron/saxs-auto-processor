import logging
import sys, os
from Worker import Worker
from Core import DatFile



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
        self.bufferIndex = None
        self.buffers = []
        self.averagedIntensities = None
        
        self.previousName = None

        
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
        if not self.buffers:
            self.bufferIndex = buffer.getIndex()
        self.buffers.append(buffer)
        self.rejection.process(self.buffers)
        self.averagedBuffer = DatFile.average([ buffer for buffer in self.buffers if buffer.isValid() ])
        datName = "avg_buffer_%s_%s" % (self.bufferIndex, buffer.getBaseFileName())
        self.averagedBuffer.datFilePath = os.path.join(self.absoluteLocation, "avg", datName)
        
        self.averagedBuffer.write()
        
        if not (self.previousName == datName):
            self.pub.send_pyobj({"command":"averaged_buffer", "location":datName})
            self.previousName = datName

    def getAveragedBuffer(self):
        if (self.averagedBuffer):
            return self.averagedBuffer
        else:
            return False
    
    def rootNameChange(self):
        pass
    
    def newBuffer(self):
        """
        | Clears out the current buffer as expecting new one.
        | Increments sample number
        """
        self.averagedBuffer = None
        self.bufferIndex = None
        self.buffers = []
    
    def clear(self):
        """
        Calls super clear() and then the workers specific requirements
        """
        Worker.clear(self)
        self.averagedBuffer = None
        self.bufferIndex = None
        self.buffers = []

        

if __name__ == "__main__":
    #Test Cases
    
    worker = WorkerBufferAverage()

     
    worker.runCommand({'command' : "averaged_buffer"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "static_image"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "averaged_buffer"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "static_image"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "averaged_buffer"})
    worker.runCommand({'command' : "shut_down"})
