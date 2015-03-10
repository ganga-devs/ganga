#!/usr/bin/env python

from optparse import OptionParser

JOB_DEFAULT = 227



if __name__ == '__main__':

    parser = OptionParser()

    parser.add_option('-I', '--id', type = 'int', dest = 'id', default = str(JOB_DEFAULT),
                      help = 'The id of the job to be collected the output. Defaults to number 0 if not specified.')
    parser.add_option('-H', '--host', type = 'string', dest = 'host', default = 'dashb-mb.cern.ch',
                      help = 'Hostname or IP to connect to. Defaults to dashb-mb.cern.ch if not specified.')
    parser.add_option('-P', '--port', type = 'int', dest = 'port', default = 61113,
                      help = 'Port providing stomp protocol connections. Defaults to 61113 if not specified.')
    parser.add_option('-U', '--user', type = 'string', dest = 'user', default = 'ganga',
                      help = 'Username for the connection')
    parser.add_option('-W', '--password', type = 'string', dest = 'password', default = 'analysis',
                      help = 'Password for the connection')
    parser.add_option('-F', '--file', type = 'string', dest = 'filename', default = 'MSGPeek-collector.log',
                      help = 'File containing the output of the jobs')
    parser.add_option('-T', '--timeout', type = 'int' , dest = 't_out', default = 3600,
                      help = 'Time out: Seconds that the client (consumer) will wait until there is a job (producer) running')

    (options, args) = parser.parse_args()

        
    from MSGPeekCollector import MSGPeekCollector as MSGPeekCollector

    collector = MSGPeekCollector(options.id ,options.host, options.port, options.filename,
                                  options.user, options.password)


#    collector.begin()


#    from time import sleep
#    print "Start"
#    collector.begin()
#    sleep(5)
#    print "Stop"
#    collector.end()
#    sleep(5)
#    print "Start"
#    collector.begin()
#    sleep(5)
#    print "Stop"
#    collector.end()
#    sleep(5)
#    print "Start"
#    collector.begin()
#    sleep(5)
#    print "Stop"
#    collector.end()
#    sleep(5)
#    print "Start"
#    collector.begin()
#    sleep(5)
#    print "Stop"
#    collector.end()
    
    
    collector.begin()
    from time import sleep
    from time import time
    start = time()
    
    while not collector.is_finished() and options.t_out > time()-start:
        try:
            if not collector.conn.is_connected():
                print 'MasterListener is not connected... attempting to reconnect...'
                collector.conn.start()
                collector.conn.connect()
                print 'Reconnected!'
            sleep(5)
#            collector.end()
#            sleep(5)
        except EOFError:
            import sys, traceback
            traceback.print_exc()
            collector.conn.stop()
        except KeyboardInterrupt:
            import sys, traceback
            traceback.print_exc()
            collector.conn.stop()

