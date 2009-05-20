import sys, time, stomp

SERVER = 'gridmsg002.cern.ch'
PORT = 6163
USERNAME = ''
PASSWD = ''

from Ganga.Utility.logging import getLogger
log = getLogger('MSGErrorLog')

# Simply log any incoming messages
class LoggerListener(object):
    def __init__(self):
        self.logger = getLogger('MSGLoggerListener')
        
    def on_error(self, headers, message):
        self.logger.debug('received an error %s' % message)

    def on_message(self, headers, message):
        self.logger.debug('received a message %s' % message)


def createPublisher(T):
    # generic connection thread class; bring up connection if necessary
    class MSGAsynchPublisher(T):
        def __init__(self,name=None):
            T.__init__(self,name=name)
            from Queue import Queue
            self.msg_q = Queue()
            self.connection = stomp.Connection([(SERVER, PORT)], USERNAME, PASSWD)
            self.connection.add_listener(LoggerListener())
            self.__should_stop_flag = False

        def run(self):
            print 'running',T
            while not self.should_stop():
                while not self.msg_q.empty():
                    if not self.connection.is_connected():
                        self.connection.start()
                        self.connection.connect()
                    m = self.msg_q.get()
                    print 'about to send',m
                    self.__send(m)
                time.sleep(0.3)
            print 'finished',T

        def send(self, (dst, msg)):
            self.msg_q.put((dst, msg))
            log.debug("Putting message %s" % msg)

        def __send(self, (dst, msg)):
            print 'sedning message',msg
            self.connection.send(destination=dst, message=repr(msg))
            log.debug("Sending message %s" % msg)
            print 'sent',msg

        def should_stop(self):
            return self.__should_stop_flag

        def stop(self):
            self.__should_stop_flag = True
    p = MSGAsynchPublisher(T)
    p.setDaemon(True)
    return p
    

# for testing purposes
if __name__ == '__main__':
    p = MSGAsynchPublisher()
    p.send(('/topic/lostman-test/status', 'Hello World 1!'))
    p.send(('/topic/lostman-test/status', 'Hello World 2!'))
    from threading import Thread
    t = Thread(target=p.run)
    t.start()
