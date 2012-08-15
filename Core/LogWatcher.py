import time
import sys
from threading import Thread



class LogWatcher():
    """
    An object that watches the specified log file for changes to the log lines,
    it returns every new line
    """
    def __init__(self):
        self.log = ""
        self.name = "LogReader"
        self.logLocation = ""
        self.alive = True
        self.thread = None
        self.callback = None

    def setLocation(self, logLocation):
        """
        Sets location of the where the log file is that we want to watch
        """
        self.logLocation = logLocation

    def setCallback(self, callback):
        """
        Sets the callback that will take every new line
        """
        self.callback = callback

    def kill(self):
        """
        Used to kill the current log watcher thread, used it we need to restart/change user
        """
        self.alive = False
        if self.thread:
            self.thread.join()

    def watch(self):
        """
        Starts a thread that watches the logfile for changes
        """
        self.thread = Thread(target=self.watchThread,)
        self.thread.start()


    def fileWatch(self):
        """
        Here we watch constantly for a new line to be created
        """
        logfile = self.logLocation
        print "Waiting for: %s" % logfile

        start_time = time.time()
        while self.alive:
            try:
                fp = open(logfile,'r')
                print fp
                break
            except IOError:
                time.sleep(0.5);
            finally:
                if time.time()-start_time > 30.0: 
                    print "Timeout waiting for: %s" % logfile
                    return                
        if self.alive:
            print "Got logfile: %s" % logfile 
        else:
            print "Killed while waiting for: %s" % logfile 
            return
            
        while self.alive:
            new = fp.readline()
            if new:
                yield new
            else:
                time.sleep(0.5)

    def watchThread(self):
        """
            This returns every new line back up to the call back
        """
        self.alive = True
        for line in self.fileWatch():
            self.callback(line)



if __name__ == "__main__":
    
    def callback(line):
        print line,

    a = LogReader()
    a.setLocation("livelogfile.log")
    a.setCallback(callback)

    a.watch()

    time.sleep(10)

    a.kill()
    print "Done"


