import logging
import sys
from Worker import Worker
from Core import AverageList
from Core import DatFile
from Core import DatFileWriter
from Pipeline import Pipeline



class WorkerRollingAverageSubtraction(Worker): 
    """
    Takes the constant stream of Static Images, averages them writes to the disk then subtracts and writes to the disk.
    It does this for every sample type
    """   
    def __init__(self, config, **kwargs):
        Worker.__init__(self, "Worker Rolling Average Subtraction")
        
        # variables for pipeline data analysis
        self.config = config
        self.nextPipelineUser = None
        self.nextPipelineExp = None
        self.nextPipelineInput = None
        self.ExperimentFolderOn = config.get("ExperimentFolderOn")
        self.PipelineOn = config.get("PipelineOn")
        
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
        
        self.rejection.process(self.datFiles)

        for datFile in self.datFiles:
            if datFile.isValid():
                intensities.append(datFile.getIntensities())
        
        datName = "avg_sample_" + str(self.datIndex) + "_" +datFile.getBaseFileName()
        averagedIntensities = self.averageList.average(intensities)
        if (averagedIntensities):
            self.datWriter.writeFile(self.absoluteLocation + "/avg/", datName, { 'q' : datFile.getq(), "i" : averagedIntensities, 'errors':datFile.getErrors()})
            self.pub.send_pyobj({"command":"averaged_sample", "location":datName, "data": zip(datFile.getq(), averagedIntensities)})
        
        
        datName = "avg_sub_sample_" + str(self.datIndex) + "_" +datFile.getBaseFileName()
        subtractedIntensities = self.subtractBuffer(averagedIntensities, self.averagedBuffer)
        if (subtractedIntensities):
            self.datWriter.writeFile(self.absoluteLocation + "/sub/", datName, { 'q' : datFile.getq(), "i" : subtractedIntensities, 'errors':datFile.getErrors()})
            self.pub.send_pyobj({"command":"averaged_subtracted_sample", "location":datName, "data": zip(datFile.getq(), subtractedIntensities)})
            
            # record the next input file ready for pipeline modelling 
            if self.PipelineOn:
                path = str(self.absoluteLocation)
                path = path.rstrip('/')
                folders = path.split('/')
                if self.ExperimentFolderOn: # user folder and experiment folder
                    self.nextPipelineUser = folders[-2]
                    self.nextPipelineExp = folders[-1]
                else: # only user folder
                    self.nextPipelineUser = folders[-1]
                    self.nextPipelineExp = None
                self.nextPipelineInput = datName


        

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
        
        # run pipeline with an averaged, subtracted datfile
        if self.nextPipelineInput and self.PipelineOn:
            # run remote pipeline
            pipeline = Pipeline.Pipeline(self.config)
            pipeline.runPipeline(self.nextPipelineUser, self.nextPipelineExp, self.nextPipelineInput)
            # empty variables since the datfile has been sent to pipeline for data processing
            self.nextPipelineUser = None
            self.nextPipelineExp = None
            self.nextPipelineInput = None
    
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
        
    worker = WorkerRollingAverageSubtraction()

    worker.runCommand({'command' : "averaged_buffer", "averaged_buffer":buffer})  #Buffer needs to be a datFile object
