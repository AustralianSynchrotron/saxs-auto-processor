import logging
import sys, os
from Worker import Worker
from Core import DatFile

class WorkerStaticImage(Worker):    
    """
    | Subtracts every image from buffer
    """
    def __init__(self, **kwargs):
        Worker.__init__(self, "Worker Static Image")
        
        #Specific Class Variables
        self.averagedBuffer = None
        
        
    def processRequest(self, command, obj):                
        command = str(obj['command'])
        
        if (command == "static_image"):
            self.logger.info("Received a static image")
            self.subtractBuffer(obj["static_image"], self.averagedBuffer)
        
        if (command == "averaged_buffer"):
            self.logger.info("Received an averaged buffer")
            try:
                self.setBuffer(obj['averaged_buffer'])
            except KeyError:
                self.logger.critical("Key Error at averaged_buffer")           

            
    
    def setBuffer(self, buffer):
        self.averagedBuffer = buffer
        self.logger.info("Set Averaged Buffer")
        
    def subtractBuffer(self, datFile, buffer):
        """Method for subtracting buffer from static sample """
        if (buffer):
            self.logger.info("Subtracting Buffer")
            datName = "raw_sub_%s" % (datFile.getFileName(), )
            subDat = datFile.subtract(buffer)
            subDat.write(os.path.join(self.absoluteLocation, 'sub', 'raw_sub', datName))
        else:
            self.logger.critical("Error with Averaged Buffer, unable to perform subtraction")    
    
    def rootNameChange(self):
        self.logger.info("Root Name Change Called - No Action Required")
        
    def newBuffer(self):
        self.averagedBuffer = None
    
    
    def clear(self):
        Worker.clear(self)
        self.averagedBuffer = None
               

if __name__ == "__main__":
    #Test Cases
        
    worker = WorkerStaticImage()
    
    dat = DatFile.DatFile("sum_data_4.dat")

    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})
    worker.runCommand({'command' : "cleddar"})

    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "static_image"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "averaged_buffer", "averaged_buffer":dat})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "static_image"})
    worker.runCommand({'command' : "test"})
    worker.runCommand({'command' : "averaged_buffer"})
    worker.runCommand({'command' : "shut_down"})
