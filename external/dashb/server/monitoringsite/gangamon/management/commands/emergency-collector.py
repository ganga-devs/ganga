import stomp, time, os.path
import traceback

import logging.handlers
logger = logging.getLogger('runcollector')
loghandler = logging.handlers.RotatingFileHandler('runcollector.log', maxBytes=1000000, backupCount=3)
logger.addHandler(loghandler)
loghandler.setFormatter(logging.Formatter("%(asctime)s: %(levelname)s: %(message)s"))
logger.setLevel(logging.INFO)
                                

# In case you need to drain the msg servers, you may use the following collector which saves messages to files.

class Main:
    help = "Runs the collector for Ganga and Diane."

    def __init__(self):
        self.connections = []

    def handle_noargs(self, **options):
        server, port = 'ganga.msg.cern.ch', 6163

        logger.info('starting collector %s %d',server, port) 

        # From Ganga 5.4.1 messages are sent to /queue/ganga.status
        # Prior to Ganga 5.4.1 messages are sent to /topic/ganga.status
        self.add_listener(EmergencyListener('ganga.emergency.log'), server, port, ['/queue/ganga.status', '/topic/ganga.status'])
        self.add_listener(EmergencyListener('ganga.uat.emergency.log'), server, port, ['/queue/ganga.status.uat', '/topic/ganga.status.uat'])
        self.add_listener(EmergencyListener('diane.emergency.log'), server, port, ['/queue/diane.journal', '/queue/diane.status'])
        self.add_listener(EmergencyListener('ganga.usage.emergency.log'), server, port, ['/queue/ganga.usage'])

        last_heartbeat_log = 0
        while True:
            if time.time()-last_heartbeat_log > 5*60: # every five minutes
                logger.info('still listening...')
                last_heartbeat_log = time.time()
                
            for (c, ts) in self.connections: # make sure connections are active
                try:
                    if not c.is_connected():
                        logger.info("%s connection not active; restarting...",c.__class__.__name__)
                        c.start()
                        c.connect()
                        for t in ts: # re-subscribe to topics
                            c.subscribe(destination=t, ack='auto')
                        logger.info('%s connected!', c.__class__.__name__)
                except (stomp.NotConnectedException, stomp.ConnectionClosedException):
                    logger.exception('%s connection error; attempting to reconnect...', c.__class__.__name__)
                # sleep
                time.sleep(1)

    def add_listener(self, listener, server, port, topics):
        connection = stomp.Connection([(server, port)])
        connection.add_listener(listener)
        connection.start()
        connection.connect()
        for t in topics:
            connection.subscribe(destination=t, ack='auto')
        self.connections.append((connection, topics))


class EmergencyListener(stomp.ConnectionListener):
    
    def __init__(self,filename):
        self.filename = filename
        import logging.handlers
        self.logger = logging.getLogger(os.path.basename(filename))
        handler = logging.handlers.RotatingFileHandler(filename, maxBytes=50000000) #50MB
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)

        
    def on_error(self, headers, body):
        logger.error('ERROR: %s, %s', repr(headers), body)

    def on_message(self, headers, body):
        try:
            logger.info('%s message %s %s', os.path.basename(self.filename),repr(headers), body)
            self.logger.info(body)
        except Exception, e:
            logger.exception('INVALID MESSAGE %s %s', headers, body)


m = Main()
m.handle_noargs()
