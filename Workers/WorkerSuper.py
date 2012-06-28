import sys
try:
    import zmq
except ImportError, e:
    print "ERROR:", e, "which is essential to run auto-processor."
    sys.exit(2)
import time
from threading import Thread
from Worker import Worker

class WorkerSuper(Worker):    
    """
    .. codeauthor:: Jack Dwyer <jackjack.dwyer@gmail.com>
    Example Worker who has inherited the base worker
    """
    def __init__(self):
        Worker.__init__(self, "Worker Super")
        

if __name__ == "__main__":
    #Test Cases
    context = zmq.Context()
    port = 1200
    
    worker = WorkerSuper()

    t = Thread(target=worker.connect, args=(port,))
    t.start()
    time.sleep(0.1)

    
    testPush = context.socket(zmq.PUSH)
    testPush.connect("tcp://127.0.0.1:"+str(port))
    print "sent command"
    testPush.send_pyobj({'command' : "test"})
    v = raw_input(">>")
    testPush.send_pyobj({'command' : "test_receive", "test_receive" : v})
    testPush.send_pyobj({'command' : "shut_down"})