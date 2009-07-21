import stomp,sys
from time import sleep

class PeekListener(stomp.ConnectionListener) :
    def __init__(self, fn):
#        self.f = file(fn, 'a')
        self.f = file(fn, 'a')
        self.name = fn
        self.finished = False

    def on_error(self, headers, message):
        print 'ERROR: %s, %s' % (repr(headers), message)

    def on_disconnected(self):
        self.f.close()

    def on_message(self, headers, message):
#        dict = eval(message)
#        dict['_msg_t'] = headers['timestamp']
##        dict['_msg_t'] = str(time.ctime(headers['timestamp']))
#        try:
#            import datetime
#            time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M.%S")
#            _col_t, event = time, dict['event']
#            #_col_t, event = time.time() , dict['event']
#        except KeyError:
#            print 'ERROR:', dict
#        try:
#            del dict['event']
#        except KeyError:
#            print 'KeyError: key \'event\' not in the dictionary'
#        data = [_col_t, event, dict]
#        """ The message will be print in the screen """
#
#        self.f.write(repr(data) + '\n')
#        self.f.flush()
#        print repr(data)

        dict = eval(message)
        if dict.has_key("stdout") :
            sys.stdout.write(dict['stdout'])
        if dict.has_key("event") :
#            print  dict["event"], self.finished
#            if dict["event"]=="submitted":
#                self.finished = False
            if dict["event"]=="finished":
                self.finished = True
                self.f.close()
#        print message


class MSGPeekCollector:

    def __init__(self, uuid, host = 'gridmsg001.cern.ch', port=6163, filename='MSGPeekcollector.log', user='', passcode=''):
        self.uuid = uuid
        self.conn = stomp.Connection([(host, port)], user, passcode)
        self.listener = PeekListener('MSGPeekcollector.log')
        self.conn.add_listener(self.listener)
        self.conn.start()
#        print "Selected job: %d" %self.uuid
        self.conn.connect()
        self.data =  '/queue/data.%d' %self.uuid
        sleep(1)
        self.conn.subscribe(destination=self.data, ack='auto')
#        self.conn.subscribe(destination='/queue/control.job.%s' %self.uuid, ack='auto')

    def begin(self):
        msg = str(self.uuid)+",begin"
        self.conn.send(msg , destination='/queue/control.job.%s' %self.uuid)
        

    def end(self):
        msg = str(self.uuid)+",end"    
        self.conn.send(msg, destination='/queue/control.job.%s' %self.uuid)                    

    def is_finished(self):
        return self.listener.finished

    def __del__(self):
        self.conn.unsubscribe(destination=self.data) 
#        self.conn.unsubscribe(destination='/queue/control.job.%s' %self.uuid)        
        self.conn.stop()
