#!/usr/bin/env python

from optparse import OptionParser

JOB_DEFAULT = 534



if __name__ == '__main__':

    parser = OptionParser()

    parser.add_option('-I', '--id', type = 'int', dest = 'id', default = JOB_DEFAULT,
                      help = 'The id of the job to be collected the output. Defaults to number 0 if not specified.')
    parser.add_option('-H', '--host', type = 'string', dest = 'host', default = 'gridmsg001.cern.ch',
                      help = 'Hostname or IP to connect to. Defaults to gridmsg001.cern.ch if not specified.')
    parser.add_option('-P', '--port', type = 'int', dest = 'port', default = 6163,
                      help = 'Port providing stomp protocol connections. Defaults to 6163 if not specified.')
    parser.add_option('-U', '--user', type = 'string', dest = 'user', default = None,
                      help = 'Username for the connection')
    parser.add_option('-W', '--password', type = 'string', dest = 'password', default = None,
                      help = 'Password for the connection')
    parser.add_option('-F', '--file', type = 'string', dest = 'filename', default = 'MSGPeek-collector.log',
                      help = 'File containing the output of the jobs')
#    parser.add_option('-S', '--seconds', type = 'int' , dest = 'sec', default = 1,
#                      help = 'Number of the seconds that a the collector has to wait in order to collect the data')

    (options, args) = parser.parse_args()

    import sys
    sys.path.append('/afs/cern.ch/user/m/mchamber/Ganga/install/Ganga-MSG-branch/python/Ganga/Lib/MonitoringServices/MSGPeek')

    import MSGPeekCollector as MSGPeekCollector

    collector = MSGPeekCollector.MSGPeekCollector(options.id ,options.host, options.port, options.filename, options.user, options.password)


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
    while not collector.is_finished() :
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
            sys.exit(1)
        except KeyboardInterrupt:
            import sys, traceback
            traceback.print_exc()
            sys.exit(1)
#    sleep(1.4*options.sec)

