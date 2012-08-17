import logging
import sys
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

        self.rejection.process(self.buffers)

        for buffer in self.buffers:
            if buffer.isValid():
                intensities.append(buffer.getIntensities())
            
        datName = "avg_buffer_" + str(self.bufferIndex) + "_" +buffer.getBaseFileName()
        self.averagedIntensities = self.averageList.average(intensities)
        
        self.datWriter.writeFile(self.absoluteLocation + "/avg/", datName, { 'q' : self.buffers[-1].getq(), "i" : self.averagedIntensities, 'errors':self.buffers[-1].getErrors()})
        
        if not (self.previousName == datName):
            self.pub.send_pyobj({"command":"averaged_buffer", "location":datName, "data": zip(buffer.getq(), self.averagedIntensities)})
            self.previousName = datName

    def getAveragedBuffer(self):
        if (self.averagedIntensities):
            return self.averagedIntensities
        else:
            return False
    
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
