import os, StringIO


def average(dat_list):
    result = DatFile()
    result.q = dat_list[0].getq()
    result.errors = dat_list[0].getErrors() # this is wrong
    result.intensities = [sum(item) / len(item) for item in zip(*[dat.getIntensities() for dat in dat_list])]
    return result

def subtract(dat_one, dat_two):    
    result = DatFile()
    result.q = dat_one.getq()
    result.errors = dat_one.getErrors()
    i_two = dat_two.getIntensities()
    result.intensities = [ val - i_two[idx] for idx, val in enumerate(dat_one.getIntensities()) ]
    return result

class _DatFile(object):
    """
    Takes a path to a datFile, parses it and calculates the respective High/Low Q's
     
    Args:
        datFilePath (String): Absolute location of the datFile as told from the local machine
    """
    def __init__(self, datFilePath=None):
        self.datFilePath = datFilePath
        self.q = []
        self.intensities = []
        self.errors = []

        self.IQL = 0.0
        self.IHQ = 0.0
        self.valid = False
        
        if datFilePath:
            self.processDatFile()
        #self.processHighLowQ()


    def getFileName(self):
        """
        | Returns the file name of the datFile
        | eg: datFile1.dat
        """
        return os.path.basename(self.datFilePath)
    
    def getIndex(self):
        filename, _ = os.path.splitext(self.getFileName())
        return filename.split("_")[-1]
    
    def getRootName(self):
        return "_".join(self.getFileName().split("_")[:-1])
    
    def getBaseFileName(self):
        """
        | Returns the base name of the sample type
        | eg: SampleType1_1555.dat becomes SampleType1.dat
        """
        return "%s.dat" % (self.getRootName(), )

    def getDatFilePath(self):
        return self.datFilePath
    
    def isValid(self):
        return self.valid
        

    def getq(self):
        """
        Returns q values
        """
        return self.q
    
    def getIntensities(self):
        """
        Returns Intensities
        """
        return self.intensities
    
    def getErrors(self):
        """
        Returns Errors
        """
        return self.errors
 
    def setValid(self, valid = False):
        self.valid = valid
        
    def processHighLowQ(self):
        self.findILQ()
        self.findIHQ()
        
    def findILQ(self, start = 3 , end = 20):
        self.ILQ  = sum(self.intensities[start:end])
        #self.ILQ = float(sum(self.ILQ)) / len(self.ILQ)
    
    def findIHQ(self, start = -20, end = -1): 
        self.IHQ = sum(self.intensities[start:end])
        
        
    def processDatFile(self):
        """
        | Process the 3 column datfile placing each column data into its correct type
        | Based off some code by Nathan Cowieson
        """

        with open(self.datFilePath) as f:
            for line in f:
                b =  line.split()
                try:
                    q = float(b[0])
                    i = float(b[1])
                    e = float(b[2])
                    self.q.append(q)
                    self.intensities.append(i)
                    self.errors.append(e)
                except IndexError:
                    pass
                except ValueError:
                    pass
            
    def getValues(self):
        """
        Returns a dictionary of all the values
        """
        return { 'q' : self.q, 'intensities' : self.intensities, 'errros' : self.errors }
        
    def reprocessDatFile(self):
        self.processDatFile()
        
    def getIHQ(self):
        """
        Returns High Q
        """
        self.findIHQ()
        return self.IHQ 
    
    def getILQ(self):
        """
        Returns Low Q
        """
        self.findILQ()
        return self.ILQ
    
    def subtract(self, buff):
        return subtract(self, buff)
        
    def write(self, datFilePath=None):
        if datFilePath:
            self.datFilePath = datFilePath
        if not self.datFilePath:
            raise Exception()
        
        f = StringIO.StringIO()
        f.write("%s\n" % self.getFileName())
        f.write("%14s %16s %16s\n" % ('q', 'I', 'Err'))
        for item in zip(self.q, self.intensities, self.errors):
            f.write("%18.10f %16.10f %16.10f\n" % item)
        
        with open(self.datFilePath, 'w') as datfile:
            datfile.write(f.getvalue())

import string
class DatFileSpeedTest(_DatFile):
    def processDatFile(self):
        with open(self.datFilePath) as f:
            # read array
            f.readline()
            f.readline()
            arr = [ line.split() for line in f ]

            # extraxt cols and convert
            self.q =           map(float, [row[0] for row in arr])
            self.intensities = map(float, [row[1] for row in arr])
            self.errors =      map(float, [row[2] for row in arr])
            
            
    
#class DatFileNumpy(DatFile):
#    def processDatFile(self):
#        self.q, self.intensities, self.errors = numpy.loadtxt(self.datFilePath, skiprows=2, unpack=True)
#class DatFileNumpy2(DatFile):
#    def processDatFile(self):
#        DatFile.processDatFile(self)
#        self.q, self.intensities, self.errors = numpy.array(self.q), numpy.array(self.intensities), numpy.array(self.errors)
#class DatFileNumpy3(DatFileSpeedTest):
#    def processDatFile(self):
#        DatFileSpeedTest.processDatFile(self)
#        self.q, self.intensities, self.errors = numpy.array(self.q), numpy.array(self.intensities), numpy.array(self.errors)
        
DatFile = DatFileSpeedTest
if __name__ == "__main__":
    b = DatFile("../data/dat/air_1_0001.dat")
    print b.getFileName()

    
