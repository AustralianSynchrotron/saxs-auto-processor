import logging
import sys
try:
    import zmq
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    #sys.exit(2) #this line terminated sphinx docs building on readthedocs.
import time
from threading import Thread
from Worker import Worker
from Core import AverageList
from Core import DatFile
from Core import DatFileWriter



class WorkerRollingAverageSubtraction(Worker): 
    """
    Takes the constant stream of Static Images, averages them writes to the disk then subtracts and writes to the disk.
    It does this for every sample type
    """   
    def __init__(self):
        Worker.__init__(self, "Worker Rolling Average Subtraction")
        
        #Class specific variables
        self.averagedBuffer = None
        self.datFiles = []
        self.datIndex = 1
        
    def processRequest(self, command, obj):
        """
        Specific worker commands
        """
        
        command = str(obj['command'])
        
        if (command == "static_image"):
            self.logger.info("Received a static image")
            self.subAvIntensities(obj['static_image'])
            #Then Subtract
        
        if (command == "averaged_buffer"):
            self.logger.info("Received an averaged buffer")
            try:
                self.setBuffer(obj['averaged_buffer'])
            except KeyError:
                self.logger.critical("Key Error at averaged_buffer")
                
    
    
    
    def subAvIntensities(self, datFile):
        """
        Averages out the datfile againse the other datfiles
        
        Args:
            datFile: the latest datfile
        """
        self.datFiles.append(datFile)
        intensities = []
        for datFile in self.datFiles:
            intensities.append(datFile.getIntensities())
        
        averagedIntensities = self.averageList.average(intensities)



        datName = "avg_sample_" + str(self.datIndex) + "_" +datFile.getBaseFileName()
        if (averagedIntensities):
            self.datWriter.writeFile(self.absoluteLocation + "/avg/", datName, { 'q' : datFile.getq(), "i" : averagedIntensities, 'errors':datFile.getErrors()})
            self.pub.send_pyobj({"command":"averaged_sample", "location":datName})


        
        datName = "avg_sub_sample_" + str(self.datIndex) + "_" +datFile.getBaseFileName()
        
        subtractedIntensities = self.subtractBuffer(averagedIntensities, self.averagedBuffer)
        
        if (subtractedIntensities):
            self.datWriter.writeFile(self.absoluteLocation + "/sub/", datName, { 'q' : datFile.getq(), "i" : subtractedIntensities, 'errors':datFile.getErrors()})
            self.pub.send_pyobj({"command":"averaged_subtracted_sample", "location":datName})


        

    def subtractBuffer(self, intensities, buffer):
        """
        Subtracts the averaged buffer from the averaged datFiles
        Currently only works on the intensities
        
        Args:
            intensities: Little cheat as it only takes intensities at the moment 
            buffer: corresponding buffer intensities
        
        """
        if (buffer):
            newIntensities = []
            for i in range(0, len(intensities)):
                newIntensities.append(intensities[i] - buffer[i])
            return newIntensities
        else:
            self.logger.critical("Error with Averaged Buffer, unable to perform subtraction")    
    
    
    
    def setBuffer(self, buffer):
        self.averagedBuffer = buffer
        self.logger.info("Set Averaged Buffer")
        
    def rootNameChange(self):
        """
        Worker needs to clear out the current datfiles and increase the sample index
        """
        self.datFiles = []
        self.datIndex = self.datIndex + 1
    
    def newBuffer(self):
        """
        needs to clear out the datFiles, and the averaged Buffer
        """
        self.datFiles = []
        self.averagedBuffer = None
        
    def clear(self):
        """
        Calls super clear(), removes averaged buffer, sets index back to 1 and clears out all datFiles
        """
        Worker.clear(self)
        self.averagedBuffer = None
        self.datFiles = []
        self.datIndex = 1
        
        
        
if __name__ == "__main__":
    #Test Cases
    context = zmq.Context()
    port = 1200
    
    worker = WorkerRollingAverageSubtraction()

    t = Thread(target=worker.connect, args=(port,))
    t.start()
    time.sleep(0.1)

    
    testPush = context.socket(zmq.PUSH)
    testPush.connect("tcp://127.0.0.1:"+str(port))
    testPush.send_pyobj({'command' : "averaged_buffer", "averaged_buffer":buffer})  #Buffer needs to be a datFile object
