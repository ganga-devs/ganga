import stomp
import sys
from time import sleep


class PeekListener(stomp.ConnectionListener) :
    def __init__(self, fn):
#        self.f = file(fn, 'a')
        self.f = open(fn, 'a')
        self.name = fn
        self.finished = False

    def on_error(self, headers, message):
        print 'ERROR: %s, %s' % (repr(headers), message)

    def on_disconnected(self):
        self.f.close()

    def on_message(self, headers, message):
        dict = eval(message)
        if dict.has_key("stdout") :
            sys.stdout.write(dict['stdout'])
        if dict.has_key("event") :
            if dict["event"] == "finished":
                self.finished = True
                self.f.close()
#        for header_key in headers.keys():
#            print '%s: %s' % (header_key, headers[header_key])
#        print message
        



class MSGPeekCollector:
    
#    from compatibility import uuid
    SessionId = '32' #uuid()
    control = '/topic/control.session.%s' %SessionId
    
    def __init__(self, uuid, host = 'dashb-mb.cern.ch', port=61113, 
                 filename='MSGPeekcollector.log', user='ganga', passcode='analysis'):
        self.uuid = uuid
        self.conn = stomp.Connection([(host, port)], user, passcode)
        self.listener = PeekListener('MSGPeekcollector.log')
        self.conn.add_listener(self.listener)
        self.conn.start()
        self.conn.connect()
        self.data =  '/queue/data.%d' %self.uuid
        self.control = '/topic/control.session.%s' %self.__class__.SessionId

        self.conn.subscribe(destination=self.data, ack='auto')

        """Possible implementation to make all the the user's jobs' status in the same topic"""
#        self.status = '/topic/job.status.%s' %self.__class__.SessionId
#        self.conn.subscribe(destination=self.status, ack='auto',
#                            headers = { 'selector' : "status  = '%s'"%self.uuid})
        """ To clean the queues with the client uncomment the lines below"""
#        self.conn.subscribe(destination=self.control, ack='auto')
#                            headers = { 'selector' : "clientid  = '%s'"%self.uuid})



    def begin(self):
        msg = {self.uuid:'begin'}
        self.conn.send(str(msg) , destination=self.control,
                       headers={ "clientid": self.uuid })

    def end(self):
        msg = {self.uuid:'end'} 
        self.conn.send(str(msg) , destination=self.control,
                       headers={ "clientid": self.uuid })

    def is_finished(self):
        return self.listener.finished
    
#    def __clean_queues(self):
#        self.conn.subscribe(destination='/queue/control.session.%s' %self.uuid, ack='auto')
#        self.conn.subscribe(destination=self.data, ack='auto')
#        sleep(1)
#        self.conn.unsubscribe(destination=self.data)
#        self.conn.unsubscribe(destination='/queue/control.session.%s' %self.uuid)
#        sleep(1)
        

    def __del__(self):
        self.conn.unsubscribe(destination=self.data) 
#        self.conn.unsubscribe(destination=self.control)       
        self.conn.stop()
