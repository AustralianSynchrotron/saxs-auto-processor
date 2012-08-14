import DatFile
import glob

class OutLierRejection():
    """
    A class that takes a list of DatFiles and changes the reference Valid value to True or False, based on Nathan Cowieson's Algorithm
    """
    
    def __init__(self):
        print "out lier rejection created"

    def process(self, datFiles):
        """ 
        Takes the list of datFiles and processses their high/low Q's and checks validity
        
        Args:
            datFiles (list[] of DatFile objects)
            
        Returns:
            Nothing
        
        Sets the reference valid value to True or False of the DatFile
        """
        if len(datFiles) == 0:
            return False
        else:
            ILQ = []
            IHQ = []
            for dat in datFiles:
                IHQ.append(dat.getIHQ())
                
            ihqThreshold = (0.92 * max(IHQ))
            
            print "IHQ Threshold: ", str(ihqThreshold)
            
            for dat in datFiles:
                if not (dat.getIHQ() < ihqThreshold):
                    dat.setValid(True)  
            
            for dat in datFiles:
                if dat.isValid():
                    ILQ.append(dat.getILQ())
    
            ilqThreshold = (1.08 * min(ILQ))
            print "IQL Threshold: ", str(ilqThreshold)
            
            for dat in datFiles:
                if (dat.getILQ() > ilqThreshold):
                    dat.setValid(False)

if __name__ == '__main__':
    outerlierTest = OutLierRejection()
    print "Running - Outlier Rejection"
    
    datC = glob.glob('C1z_buffer_1p6m_0020.dat')
    datD = glob.glob('C1active_buffer*')
    datF = glob.glob('C1q_buffer*')
    
    dC = []
    dD = []
    dF = []

    for files in datC:
        dC.append(DatFile.DatFile(files))
    outerlierTest.process(dC)

    for dat in dC:
        print dat.getFileName(), dat.isValid(), dat.getIHQ(), dat.getILQ()


    for files in datD:
        dD.append(DatFile.DatFile(files))
    outerlierTest.process(dD)

    for dat in dD:
        print dat.getFileName(), dat.isValid(), dat.getIHQ(), dat.getILQ()

    for files in datF:
        dF.append(DatFile.DatFile(files))
    outerlierTest.process(dF)

    for dat in dF:
        print dat.getFileName(), dat.isValid(), dat.getIHQ(), dat.getILQ()
