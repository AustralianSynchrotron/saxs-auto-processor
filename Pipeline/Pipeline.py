#!/usr/bin/python

import os
import subprocess
import sys
import time
import getopt
import logging

try:
    import yaml
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    #sys.exit(2) #this line terminated sphinx docs building on readthedocs.


class Pipeline:
    def __init__(self, configuration, username, experiment, datfile):
        self.name = "Pipeline"
        
        #---------- Auto processor settings -----------------------------------#
        self.PROD_USER                  = None
        self.PROD_HOST                  = None
        self.PROD_CODE_ROOT             = None
        self.PROD_DATA_ROOT             = None
        self.PROD_PIPELINE_HARVEST      = None
        self.PROD_USER_EPN              = username
        self.PROD_USER_EXP              = experiment
        self.PROD_USER_DAT_FILE         = datfile
        self.PROD_PIPELINE_INPUT_DIR    = None
        self.PROD_PIPELINE_OUTPUT_DIR   = None
        self.PROD_SSH_ACCESS            = None
        self.PROD_PIPELINE_SCP_FROM     = None
        self.PROD_PIPELINE_SCP_DEST     = None
        self.PROD_PIPELINE_HARVEST_PATH = None
        #---------- Pipeline settings -----------------------------------------#
        self.MASSIVE_USER               = None
        self.MASSIVE_HOST               = None
        self.PIPELINE_CODE_ROOT         = None
        self.PIPELINE_DATA_ROOT         = None
        self.PIPELINE_WRAPPER           = None
        self.PIPELINE_INPUT_DIR         = None
        self.PIPELINE_OUTPUT_DIR        = None
        self.PIPELINE_USER_EXP_DIR      = None
        self.PIPELINE_USER_INPUT_DIR    = None
        self.PIPELINE_USER_OUTPUT_DIR   = None
        
        #Read all configuration settings
        self.setConfiguration(configuration)


    def setConfiguration(self, configuration):
        try:
            stream = file(configuration, 'r') 
        except IOError:
            logging.critical(self.name, "Unable to find configuration file settings.conf, exiting.")
            sys.exit
        
        config = yaml.load(stream)
        
        #---------- Auto processor settings -----------------------------------#
        self.PROD_USER = config.get('PROD_USER')
        self.PROD_HOST = config.get('PROD_HOST')
        
        self.PROD_CODE_ROOT = config.get('PROD_CODE_ROOT')
        self.PROD_DATA_ROOT = config.get('PROD_DATA_ROOT')
        self.PROD_PIPELINE_HARVEST = config.get('PROD_PIPELINE_HARVEST')
        
        self.PROD_PIPELINE_INPUT_DIR = config.get('PROD_PIPELINE_INPUT_DIR')
        self.PROD_PIPELINE_OUTPUT_DIR = config.get('PROD_PIPELINE_OUTPUT_DIR')
        
        self.PROD_SSH_ACCESS = self.PROD_USER + "@" + self.PROD_HOST
        self.PROD_PIPELINE_SCP_FROM = self.PROD_DATA_ROOT + "/" + self.PROD_USER_EPN + "/" + self.PROD_USER_EXP + "/" + self.PROD_PIPELINE_INPUT_DIR
        self.PROD_PIPELINE_SCP_DEST = self.PROD_DATA_ROOT + "/" + self.PROD_USER_EPN + "/" + self.PROD_USER_EXP + "/" + self.PROD_PIPELINE_OUTPUT_DIR        
        self.PROD_PIPELINE_HARVEST_PATH = self.PROD_CODE_ROOT + "/" + self.PROD_PIPELINE_HARVEST
        
        #---------- Pipeline settings -----------------------------------------#
        self.MASSIVE_USER = config.get('MASSIVE_USER')
        self.MASSIVE_HOST = config.get('MASSIVE_HOST')
        self.PIPELINE_CODE_ROOT = config.get('PIPELINE_CODE_ROOT')
        self.PIPELINE_DATA_ROOT = config.get('PIPELINE_DATA_ROOT')
        self.PIPELINE_WRAPPER = config.get('PIPELINE_WRAPPER')
        
        self.PIPELINE_INPUT_DIR = self.PROD_PIPELINE_INPUT_DIR
        self.PIPELINE_OUTPUT_DIR = self.PROD_PIPELINE_OUTPUT_DIR

        self.PIPELINE_USER_EXP_DIR = self.PIPELINE_DATA_ROOT + "/" + self.PROD_USER_EPN + "/" + self.PROD_USER_EXP
        self.PIPELINE_USER_INPUT_DIR = self.PIPELINE_USER_EXP_DIR + "/" + self.PIPELINE_INPUT_DIR
        self.PIPELINE_USER_OUTPUT_DIR = self.PIPELINE_USER_EXP_DIR + "/" + self.PIPELINE_OUTPUT_DIR + "/"


    def runPipeline(self):
        # Create user's directories
        self.createDirs()
        # Copy user's experimental data file
        self.copyDatfile()
        # Trigger pipeline wrapper script on MASSIVE 
        self.triggerPipelineWrapper()
        
        
    def createDirs(self):
        # create user pipeline input directory
        command = "ssh %s@%s mkdir -p %s" % (self.MASSIVE_USER, self.MASSIVE_HOST, self.PIPELINE_USER_INPUT_DIR)
        os.system(command)
        # create user pipeline output directory
        command = "ssh %s@%s mkdir -p %s" % (self.MASSIVE_USER, self.MASSIVE_HOST, self.PIPELINE_USER_OUTPUT_DIR)
        os.system(command)

    
    def copyDatfile(self):
        # copy a local data file on production to a remote massive host
        command = "scp %s/%s %s@%s:%s/%s" % (self.PROD_PIPELINE_SCP_FROM, 
                                             self.PROD_USER_DAT_FILE, 
                                             self.MASSIVE_USER, 
                                             self.MASSIVE_HOST, 
                                             self.PIPELINE_USER_INPUT_DIR, 
                                             self.PROD_USER_DAT_FILE)
        os.system(command)
    
    def triggerPipelineWrapper(self):
        #---------- Pipeline modelling ----------------------------------------#
        # Submit pipeline process jobs on to massive computing cluster server.
        # Arguments for pipeline wrapper:
        #   ARG1: A full path of user's experimental data file to be used for 
        #         models.
        #   ARG2: A full directory path for all output files generated during 
        #         pipeline modelling.
        #   ARG3: A string of ssh username and remote hostname used to connect 
        #         to SAXS production server, ex. username@hostname.
        #   ARG4: A remote full directory path for pipeline to copy output files 
        #         back to remote SAXS production server.
        #   ARG5: A remote full path to trigger pipeline harvest script on 
        #         remote SAXS production server.
        #   ARG6: A full absolute path of home directory of Pipeline source code 
        #         on MASSIVE.
        
        ARG1=self.PIPELINE_USER_INPUT_DIR + "/" + self.PROD_USER_DAT_FILE
        ARG2=self.PIPELINE_USER_OUTPUT_DIR
        ARG3=self.PROD_SSH_ACCESS
        ARG4=self.PROD_PIPELINE_SCP_DEST
        ARG5=self.PROD_PIPELINE_HARVEST_PATH
        ARG6=self.PIPELINE_CODE_ROOT
        
        # start pipeline analysis
        command = "ssh %s@%s bash %s/%s %s %s %s %s %s %s" % (self.MASSIVE_USER,
                                                              self.MASSIVE_HOST,
                                                              self.PIPELINE_CODE_ROOT,
                                                              self.PIPELINE_WRAPPER,
                                                              ARG1,
                                                              ARG2,
                                                              ARG3,
                                                              ARG4,
                                                              ARG5,
                                                              ARG6)
        os.system(command)


def usage():
    """
    Usage: ./Pipeline.py -c "/full/path/configfile" -u -"username" -e "experiment" -d "/full/path/datfile.dat" 
    
    -c --config      The full path of configurtion file which holds all settings
                     about how to remotely run pipeline on MASSIVE service. 
    -u --username    The folder name of user's name with EPN.
    -e --experiment  The folder name of user's experiment.
    -d --datfile     The full path of your SAXS experimental data file to be used 
                     for models.
    """
    print 'Usage: %s -c "/full/path/configfile" -u -"username" -e "experiment" -d "/full/path/datfile.dat"   \n' % (sys.argv[0])
    print '''
-c --config      The full path of configurtion file which holds all settings
                 about how to remotely run pipeline on MASSIVE service. 
-u --username    The folder name of user's name with EPN.
-e --experiment  The folder name of user's experiment.
-d --datfile     The full path of your SAXS experimental data file to be used 
                 for models.

'''    


if __name__ == "__main__":
    configuration = ""
    username = ""
    experiment = ""
    datfile = ""
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "c:u:e:d:", ["config", "user", "exp", "datfile"])
    except getopt.GetoptError, err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    # get prefix option, example: -p /data_home/user_epn/user_exp/analysis/sample        
    for o, a in opts:
        if o in ("-c", "--conf"):
            configuration = str(a)
        if o in ("-u", "--user"):
            username = str(a)
        if o in ("-e", "--exp"):
            experiment = str(a)
        if o in ("-d", "--datfile"):
            datfile = str(a)

    if not datfile.endswith('.dat'):
        print "ERROR: *.dat file (SAXS experimental data file) is expected as an input file."
        sys.exit(2) 
    
    
    pipeline = Pipeline(configuration, username, experiment, datfile)
    pipeline.runPipeline()
    
    