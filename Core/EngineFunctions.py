"""
   Collection of Functions used by the Engine
"""

import os


def getString(string, index):
    """
    Splits the string by / and empty chars. Was orginally for returning user, but now it is used to get user and experiment
    """
    values = string.split("/")
    values = filter(None, values) #needed to remove the none characters from the array
    
    ###########################################################################################################
    #set to -2
    return values[index] #
    ###########################################################################################################
    

def testStringChange(string, previousString = None):
    """
    Tests against 2 strings, orginally was to check if there had been a change in user, now it is used to check against user and expierment
    """
    if not (previousString):
        return True
    else:
        if (previousString == string):
            return False
        if (previousString != string):
            return True
        
def createFolderStructure(fullPath):
    """
    Creates Directory structure if it is not already present
    """
    try:
        os.makedirs(fullPath)
    except OSError:
        pass
    try:
        os.makedirs(fullPath + "/images/")
    except OSError:
        pass
    try:
        os.makedirs(fullPath + "/raw_dat/")
    except OSError:
        pass
    try:
        os.makedirs(fullPath + "/avg/")
    except OSError:
        pass
    try:
        os.makedirs(fullPath + "/sub/")
    except OSError:
        pass
    try:
        os.makedirs(fullPath + "/sub/raw_sub/")
    except OSError:
        pass
    try:
        os.makedirs(fullPath + "/analysis/")
    except OSError:
        pass
        #os.makedirs(fullPath + "/manual/")
        #os.makedirs(fullPath + "/manual/man_raw_dat/")
        #os.makedirs(fullPath + "/manual/man_avg/")
        #os.makedirs(fullPath + "/manual/man_sub/")
        #os.makedirs(fullPath + "/manual/man_sub/man_raw_sub/")
        #os.makedirs(fullPath + "/manual/man_analysis/")
    except OSError:
        pass

