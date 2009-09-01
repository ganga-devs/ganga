import sys, time, stomp

from Queue import Queue
msg_q = Queue() # send() -> producer, connection thread -> consumer

server, port = 'gridmsg002.cern.ch', 6163

from Ganga.Utility.logging import getLogger
err_log = getLogger('MSGErrorLog')

# Simply log any incoming messages
class LoggerListener(object):
    def __init__(self):
        self.logger = getLogger('MSGLoggerListener')
        
    def on_error(self, headers, message):
        self.logger.debug('received an error %s' % message)

    def on_message(self, headers, message):
        self.logger.debug('received a message %s' % message)

# generic connection thread class; bring up connection if necessary
class MSGBaseConnectionThread:
    def __init__(self):
        self.connection = stomp.Connection([(server, port)], '', '')
        self.connection.add_listener(LoggerListener())
        self.connection.start()
        self.connection.connect()
        
    def run(self):
        while not self.should_stop():
            #if msg_q.empty():
            #    continue
            (msg, dst) = msg_q.get()
            # No connection to MSG server exists; reconnect (locking call)
            if not self.connection.is_connected():
                self.connection.start()
            self.connection.send(destination=dst, message=repr(msg))

try: # client side -> use GangaThread class
    from Ganga.Core.GangaThread import GangaThread
    class MSGConnectionThread(MSGBaseConnectionThread, GangaThread):
        def __init__(self):
            MSGBaseConnectionThread.__init__(self)
            GangaThread.__init__(self, 'MSGConnectionThread')

    c = MSGConnectionThread()
    c.setDaemon(True)

except: # worker side -> use simple python Thread class
    from threading import Thread
    class MSGConnectionThread(MSGBaseConnectionThread, Thread):
        def __init__(self):
            MSGBaseConnectionThread.__init__(self)
            Thread.__init__(self)
            self.__should_stop_flag = False

        def should_stop(self):
            return self.__should_stop_flag

        def stop(self):
            if not self.__should_stop_flag:
                #logger.debug("Stopping: %s",self.getName())
                self.__should_stop_flag = True

    c = MSGConnectionThread()
    c.setDaemon(True)
c.start()


def send(msg, dst): # enqueue the msg in msg_q for the connection thread to consume and send
    msg_q.put((msg, dst)) 

def sendJobStatusChange(msg):
    send(msg, '/topic/lostman-test/status')
    
#def sendJobStatusChange(jobid, backend, status):
#    msg = '{ "jobid":"%s", "backend":"%s", "status":"%s" }' % (jobid, backend, status)
#    connection.send(destination='/topic/lostman-test/status', message=msg)

def sendJobSubmitted(msg):
    send(msg, '/topic/lostman-test/submitted')

#def sendJobSubmitted(name, id, time, app, tasks, wagents):
#    msg = '{ "name":"%s", "runid":%d, "time":%f, "application":"%s", "tasks":%d, "wagents":%d }' % (name, id, time, app, tasks, wagents)
#    connection.send(destination='/topic/lostman-test/submitted', message=msg)
