"""
Jack Dwyer
31 March 2012

Common DatFileWriter averaging class/methods.  

expects a dictionary with q, i, error values
prints to an accuracy of 10 decimal places.

"""


class DatFileWriter:
    
    def __init__(self, location, name, dict, accuracy = 10):
        self.location = location
        self.name = name
        self.dict = dict
        self.accuracy = accuracy
        
        
        
        
    def writeFile(self):
        loc = self.location+self.name
        f = open(loc, 'w')
        f.write(self.name + "\n")
        
        formatting  = '%'+str(4 + self.accuracy)+'s %'+str(6 + self.accuracy)+'s %'+str(6 + self.accuracy)+'s \n'
        f.write(formatting % ('q', 'I', 'Err')) #Needed for string formatting
        for i in range(len(self.dict['q'])):
            formatting = '%'+str(8 + self.accuracy)+'.'+str(self.accuracy)+'f %'+str(6 + self.accuracy)+'.'+str(self.accuracy)+'f %'+str(6 + self.accuracy)+'.'+str(self.accuracy)+'f \n'
            f.write(formatting % (self.dict['q'][i], self.dict['i'][i], self.dict['errors'][i]))        
        f.close()
        print "file written"

        
#test
if __name__ == "__main__":
    data = { 'q' : [0, 1, 4, 5, 6, 8], 'i' : [ 8, 5, 7, 7, 6, 7], 'errors' : [1, 3, 5, 6, 3, 5, 7]}
    testWrite = DatFileWriter("unittests/", "test.dat", data)
    testWrite.writeFile()