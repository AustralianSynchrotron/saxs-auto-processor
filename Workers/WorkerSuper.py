import sys
from Worker import Worker

class WorkerSuper(Worker):    
    """
    Example Worker who has inherited the base worker
    """
    def __init__(self, **kwargs):
        Worker.__init__(self, "Worker Super")
        

if __name__ == "__main__":
    #Test Cases
        
    worker = WorkerSuper()

    print "sent command"
    worker.runCommand({'command' : "test"})
    v = raw_input(">>")
    worker.runCommand({'command' : "test_receive", "test_receive" : v})
    worker.runCommand({'command' : "shut_down"})
