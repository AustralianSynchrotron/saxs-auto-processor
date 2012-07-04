#!/usr/bin/python

import getopt
import glob
import sys

def main():
    type = ""
    file_to_harvest = ""
    prefix = ""
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "t:f:p:", ["type", "file", "prefix"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    # get prefix option, example: -p /data_home/user_epn/user_exp/analysis/sample        
    for o, a in opts:
        if o in ("-t", "--type"):
            type = str(a)
        if o in ("-f", "--file"):
            file_to_harvest = str(a)
        if o in ("-p", "--prefix"):
            prefix = str(a)

    if type not in ["porod_volume", "dammif_volume", "damaver"]:
        print "ERROR: Invalid type '%s'. One of 'porod_volume', 'dammif_volume', or 'damaver'." % (type)
        sys.exit(2)
        
    harvestFile(type, file_to_harvest, prefix)


def harvestFile(type, file_to_harvest, prefix):
    if type == "porod_volume":
        if os.path.isfile(file_to_harvest):
            porodfile = open(file_to_harvest, 'r')
            value = str(porodfile.read())
            porodfile.close()
            # save "porod volume" value to database
            # ... Todo on auto processor ...
        
    elif type == "dammif_volume":
        if os.path.isfile(file_to_harvest):
            dammiffile = open(file_to_harvest, 'r')
            value = str(dammiffile.read())
            dammiffile.close()
            # save "Total excluded DAM volume" value to database
            # ... Todo on auto processor ...
            
        

def usage():
    print 'Usage: %s [OPTIONS] -p /data_home/user_epn/user_exp/analysis/sample  \n' % (sys.argv[0])
    print '''
-p --prefix    Prefix to prepend to output filenames (*-1.pdb). Should include 
               absolute paths, all directory components must exist.


'''
  

if __name__ == "__main__":
    main()
        