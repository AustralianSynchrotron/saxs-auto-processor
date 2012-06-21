import os


class DatFileWriter:
    """
    .. codeauthor:: Jack Dwyer <jackjack.dwyer@gmail.com>
    An class that writes out datFiles created into a similar format as scatterbrain
    
    Args:
        accuracy (int, default=10): Can specify how many decimal places you want it to write out to
    """
    
    
    def __init__(self, accuracy = 10):
        self.name = "DatFileWriter"
        self.location = ""
        self.datName = ""
        self.data = {}
        self.accuracy = accuracy
   
        
    def writeFile(self, location, datName, dict):
        """
        Is called when you want the objec to write out a dictionary of values to the specified datFile
        
        Args:
            | location (String): The absolute location specified from the Worker/Engine as to where the datFile is to be written
            | datName (String): Name for the Datfile
            | dict (Dictionary): Takes a dictionary of q, i, errors
        
        """
            
            
        self.location = location
        self.datName = datName
        self.data = dict
        
        #Used for checking if the folder exists.. will be used later in engine
        if not os.path.exists(self.location):
            os.makedirs(self.location)
                   
        loc = self.location+self.datName                
        f = open(loc, 'w')
        f.write(self.datName + "\n")
        formatting  = '%'+str(4 + self.accuracy)+'s %'+str(6 + self.accuracy)+'s %'+str(6 + self.accuracy)+'s \n'
        f.write(formatting % ('q', 'I', 'Err')) #Needed for string formatting
        for i in range(len(self.data['q'])):
            formatting = '%'+str(8 + self.accuracy)+'.'+str(self.accuracy)+'f %'+str(6 + self.accuracy)+'.'+str(self.accuracy)+'f %'+str(6 + self.accuracy)+'.'+str(self.accuracy)+'f \n'
            f.write(formatting % (self.data['q'][i], self.data['i'][i], self.data['errors'][i]))        
        f.close()
	#TODO: add to log what was written

        
#test
if __name__ == "__main__":
    data = { 'q' : [0, 1, 4, 5, 6, 8], 'i' : [ 8, 5, 7, 7, 6, 7], 'errors' : [1, 3, 5, 6, 3, 5, 7]}
    testWrite = DatFileWriter()
    testWrite.writeFile("unittests/", "test.dat", data)
