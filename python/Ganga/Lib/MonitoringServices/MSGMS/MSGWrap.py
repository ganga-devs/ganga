import sys, time, stomp

from Ganga.Utility.logging import getLogger

# Simply log any incoming messages

class LoggerListener(object):
    def __init__(self):
        self.logger = getLogger('MSGLoggerListener')
        
    def on_error(self, headers, message):
        self.logger.debug('received an error %s' % message)

    def on_message(self, headers, message):
        self.logger.debug('received a message %s' % message)

err_log = getLogger('MSGErrorLog')

server, port = 'gridmsg002.cern.ch', 6163

connection = stomp.Connection([(server, port)], '', '')
connection.add_listener(LoggerListener())
connection.start()
connection.connect()

def send(msg):
    try:
        connection.send(destination='/topic/lostman-test', message=msg)
    except:
        # No connection to MSG server exists; log an error
        err_log.error('Unable to connect to MSG server. Message: "%s" undelivered.' % msg)
        

def sendJobStatusChange(jobid, backend, status):
    msg = '{ "jobid":"%s", "backend":"%s", "status":"%s" }' % (jobid, backend, status)
    connection.send(destination='/topic/lostman-test/status', message=msg)

def sendJobSubmitted(name, id, time, app, tasks, wagents):
    msg = '{ "name":"%s", "runid":%d, "time":%f, "application":"%s", "tasks":%d, "wagents":%d }' % (name, id, time, app, tasks, wagents)
    connection.send(destination='/topic/lostman-test/submitted', message=msg)
