#!/usr/bin/env python
from multiprocessing.managers import BaseManager
import collections
from Queue import PriorityQueue

# Can use this if need the definition of the SocketAddress tuple later
SocketAddress = collections.namedtuple('SocketAddress', ['address',  'port'])
QueueElement  = collections.namedtuple('QueueElement',  ['priority', 'client_socket', 'command', 'timeout'])

class QueueManager(BaseManager):
    def __init__(self):
        super(QueueManager,self).__init__()
        self.__queue = PriorityQueue()
        QueueManager.register('get_queue',callable=lambda:self.__queue)
        QueueManager.register('queue',    callable=lambda:str(self.__queue.queue))


class TimeoutException(Exception):pass

class DiracServer(object):
    import os, inspect, multiprocessing, re, threading, thread, sys, socket, pickle
    
    __slots__ = ['__end_data_str','__server_shutdown_str','__command_timeout', '__qm',
                 '__ports','__modules','__num_worker_threads']

    ########################################################################################
    def __get_port_list(self, port_min, port_max):
        import re
        regex = re.compile('[0-9]+/[a-z]{3}')
        try:
            f=open('/etc/services','r')
            inUsePorts=set( (int(l.split('/')[0]) for l in regex.findall(f.read())) )
            f.close()
        except:
            inUsePorts=set([])

        userPorts=set( range(port_min, port_max+1) )#port_max+1 as want to include port_max
        return userPorts.difference(inUsePorts)
    ########################################################################################


    def __init__( self,
                  port_min            = 49000,
                  port_max            = 65000,
                  command_files       = set([]),
                  command_timeout     = 60,
                  num_worker_threads  = 5,
                  end_data_str        = '###END-DATA-TRANS###',
                  server_shutdown_str = '###SERVER-SHUTDOWN###' ):
        
        self.__qm = QueueManager()
        ## Add the default commands. Do it this way rather than add straight to __modules to avoid duplication
        command_files.add(DiracServer.os.path.join( DiracServer.os.path.dirname(DiracServer.os.path.realpath(DiracServer.inspect.getsourcefile(DiracServer))),
                                                    'DiracCommands.py' ))
        
        self.__ports               = self.__get_port_list(port_min, port_max)
        self.__end_data_str        = end_data_str
        self.__server_shutdown_str = server_shutdown_str
        self.__command_timeout     = command_timeout
        #self.__queue               = DiracServer.Queue.PriorityQueue()
        self.__num_worker_threads  = num_worker_threads
        #self.__run_workers         = True
        self.__modules             = []
        #self.__port_max            = port_max
        
        ## Check that the command files are present
        for file in command_files:
            if not DiracServer.os.path.exists(file):
                print "ERROR: Specified commands file %s doesn't exist." % file
                DiracServer.sys.exit(1)
                
            path, module = DiracServer.os.path.split(file)
            self.__modules.append( (path, module.split('.')[0]) )

    def __worker_func(self, qm):
        import copy,signal,traceback, pickle, socket
        from multiprocessing.reduction import rebuild_handle
        
        def timeout_handler(signum, frame):
            raise TimeoutException()

        ns = {}
        #print self.__modules
        exec 'import sys' in {}, ns
        for path, module in self.__modules:
            exec "sys.path.append('%s')" % path in {}, ns
            exec 'import %s' % module in {}, ns
        old_handler = signal.signal(signal.SIGALRM, timeout_handler)#could go outside while loop
        while True:
            item = qm.get_queue().get()
            #print item
            if item.command == self.__server_shutdown_str:
                #self.__close_socket(item.client_socket)
                qm.get_queue().task_done()              
                break
            tmp_ns = copy.copy(ns)
            fd = rebuild_handle(item.client_socket)
            client_socket = socket.fromfd(fd, socket.AF_INET, socket.SOCK_STREAM)
            signal.alarm(item.timeout)
            try:
                print "INFO: Executing command '%s'" % item.command
                exec item.command in {}, tmp_ns
                #print tmp_ns
                signal.alarm(0) #cancel the alarm
                client_socket.sendall(pickle.dumps(tmp_ns.get('result',"Unknown problem retreiving result of DIRAC commnd '%s'"%item.command)) + self.__end_data_str)
#                item.client_socket.sendall(pickle.dumps(ns.get('result','Unknown problem retreiving result of DIRAC commnd %s'%item.command)) + self.__end_data_str)
            except TimeoutException:
                signal.alarm(0) #cancel the alarm
                print "WARNING: Timed out executing command '%s'" % item.command
                client_socket.sendall(pickle.dumps("Timed out executing command '%s'\n###TIMEOUT###" % item.command) + self.__end_data_str)
#                item.client_socket.sendall(pickle.dumps('Timed out executing command \'%s\'\n###TIMEOUT###' % item.command) + self.__end_data_str)
            except:
                signal.alarm(0) #cancel the alarm
                print "ERROR: Exception executing command '%s'" % item.command
                client_socket.sendall(pickle.dumps(traceback.format_exc()+'###TRACEBACK###') + self.__end_data_str)
#                item.client_socket.sendall(pickle.dumps(traceback.format_exc()+'###TRACEBACK###') + self.__end_data_str)

            #signal.signal(signal.SIGALRM, old_handler)#reset the signal handler#not necessary
            self.__close_socket(client_socket)
#            self.__close_socket(item.client_socket)
            qm.get_queue().task_done()

    def __stop_workers(self, processes):
        for i in range(processes):
            self.__qm.get_queue().put(QueueElement(0,'',self.__server_shutdown_str,15))
            
    def __close_socket(self, socket):
        socket.shutdown(DiracServer.socket.SHUT_RDWR)
        socket.close()

    def __dispatcher_thread(self, client_socket, server_addr):
        ## Listen for commands
        cmd = ''
        priority_regex = DiracServer.re.compile('###PRIORITY=([0-9]+?)###')
        timeout_regex  = DiracServer.re.compile('###TIMEOUT=([0-9]+?)###')
        from multiprocessing.reduction import reduce_handle
        while True:
            data = client_socket.recv(1024)
            if not data:
                self.__close_socket(client_socket)
                break # exit and close socket as other end dead
            cmd += data
            if(cmd.find(self.__server_shutdown_str) >= 0):# other end sent shutdown string
                DiracServer.thread.interrupt_main()
                ## Horrible hack but the interrupt exception isn't read until it finishes the accept line, SOME INTERRUPT!
                ###############################################################################
                hack_client_socket = DiracServer.socket.socket(DiracServer.socket.AF_INET,
                                                               DiracServer.socket.SOCK_STREAM)
                try:
                    hack_client_socket.connect(server_addr)
                    self.__close_socket(hack_client_socket)
                except: pass
                ###############################################################################
                self.__close_socket(client_socket)
                break
            if(cmd.find(self.__end_data_str) >= 0):# found end data string
                if(cmd.find('###GET-QUEUE###') >= 0):# other end asks for current queue state
                    client_socket.sendall( DiracServer.pickle.dumps(self.__qm.queue()._getvalue()) + self.__end_data_str )
                    self.__close_socket(client_socket)
                    break
                if priority_regex.search(cmd) is None:
                    print "WARNING: Found no priority value for command '%s'. Attaching a medium priority of 5" % cmd.replace(self.__end_data_str,'')
                    cmd += '###PRIORITY=5###'
                if len(priority_regex.search(cmd).groups()) != 1:
                    print "ERROR: Found more than one priority value. Abandoning request."
                    client_socket.sendall( DiracServer.pickle.dumps('More than one priority index found, contact a ganga developer.') + self.__end_data_str )
                    self.__close_socket(client_socket)
                    break
                if timeout_regex.search(cmd) is None:
                    print "INFO: Found no timeout value for command '%s'. Attaching default timeout of %i" % (cmd.replace(self.__end_data_str,'') , self.__command_timeout)
                    cmd += '###TIMEOUT=%i###' % self.__command_timeout
                if len(timeout_regex.search(cmd).groups()) != 1:
                    print "ERROR: Found more than one timeout value. Abandoning request."
                    client_socket.sendall( DiracServer.pickle.dumps('More than one timeout index found, contact a ganga developer.') + self.__end_data_str )
                    self.__close_socket(client_socket)
                    break
                priority = int(priority_regex.search(cmd).groups()[0])
                timeout  = int(timeout_regex.search(cmd).groups()[0])
                self.__qm.get_queue().put( QueueElement(priority      = priority,
                                                        client_socket = reduce_handle(client_socket.fileno()),
                                                        command       = cmd.replace(timeout_regex.search(cmd).group(),'').replace(priority_regex.search(cmd).group(),'').replace(self.__end_data_str,''),
                                                        timeout       = timeout
                                                        ) )
                break


    def start(self, fd=None):
        ## Setup the socket and start listening
        server_socket = DiracServer.socket.socket(DiracServer.socket.AF_INET,
                                                  DiracServer.socket.SOCK_STREAM)
        server_socket.setsockopt(DiracServer.socket.SOL_SOCKET, DiracServer.socket.SO_REUSEADDR, 1)# allow reuse of socket without waiting for matural timeout to expire
        for port in self.__ports:
            socket_addr = SocketAddress(address = 'localhost', port = port)
            try:
                server_socket.bind(socket_addr)
                print "INFO: Listening on %s" % str(socket_addr)
                break
            except:
                print "ERROR: Couldn't bind to %s" % str(socket_addr)
                if port == list(self.__ports)[-1]:
                    raise Exception('Couldn\'t establish DIRAC server on any of the ports in the range')
        if fd is not None:
            sockB = DiracServer.socket.fromfd(fd,DiracServer.socket.AF_UNIX,DiracServer.socket.SOCK_STREAM)
            sockB.sendall(str(port))
            self.__close_socket(sockB)
        server_socket.listen(1) # will need multiple servers an not multithreaded

        # Start the QueueManager 
        self.__qm.start()

        ## Setup worker process pool
        processes = self.__num_worker_threads
        if self.__num_worker_threads == 0:
            processes = DiracServer.multiprocessing.cpu_count()
        pool =  DiracServer.multiprocessing.Pool( processes        = processes,
                                                  initializer      = self.__worker_func,
                                                  initargs         = (self.__qm,),
                                                  #maxtasksperchild = None #python 2.7
                                                  )

        ## Run the server loop and deal with incomming client requests
        while True:
            try:
                client_socket, client_address = server_socket.accept()
                print "INFO: Connection accepted (from address=%r, on port=%r)" % client_address
                DiracServer.threading.Thread( target=self.__dispatcher_thread,
                                              args=(client_socket,
                                                    SocketAddress(address = 'localhost', port = port))#, DiracServer.copy.copy(commandNamespace))#this didnt work with deepcopy
                                              ).start()
            except KeyboardInterrupt: break ## interrupted when server shutdown sent 
        

        ## Shutdown the server
        print "INFO: Server shutting down"
        self.__close_socket(server_socket)
        pool.close()# can put this right after the setting up of pool as we submit our jobs via the queue not the pool apply mechanism
        self.__stop_workers(processes)
        pool.join()
        self.__qm.shutdown()

# Standalone server functions
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def start_server( env_file            = None,
                  port_min            = 49000,
                  port_max            = 65000,
                  command_files       = set([]),
                  command_timeout     = 60,
                  num_worker_threads  = 5,
                  end_data_str        = '###END-DATA-TRANS###',
                  server_shutdown_str = '###SERVER-SHUTDOWN###',
                  poll_delay          = 1,
                  show_dirac_output   = False):

    import inspect, os, subprocess, sys, socket, time

    ## Add the default commands. ##not needed as this happens anyway with the option parser
    #command_files.add(os.path.join( os.path.dirname(os.path.realpath(inspect.getsourcefile(DiracServer))),
    #                               'DiracCommands.py' )
    env=None
    if env_file is not None:
        if not os.path.exists(env_file):
            print "ERROR: Environment file for server '%s' not found" % env_file
            sys.exit(4)
        else:
            f=open(env_file,'r')
            lines = f.readlines()
            f.close()
            env = dict(zip([line.split('=',1)[0] for line in lines if len(line.split('=',1)) == 2],
                           [line.split('=',1)[1].replace('\n','') for line in lines if len(line.split('=',1)) == 2]
                           )
                       )

    stdout = open('/dev/null','w')
    if show_dirac_output is True:
        stdout = None


    sockA, sockB = socket.socketpair()    
    process = subprocess.Popen( '%s -r%s -p%s -x%s -n%s -t%s -e%s -s%s %s' % ( os.path.realpath(inspect.getsourcefile(DiracServer)),
                                                                               sockB.fileno(),
                                                                               port_min,
                                                                               port_max,
                                                                               num_worker_threads,
                                                                               command_timeout,
                                                                               end_data_str,
                                                                               server_shutdown_str,
                                                                               ' '.join(('-c '+cmd for cmd in command_files)) ),
                                shell      = True,
                                env        = env,
                                cwd        = None,#os.path.dirname(os.path.realpath(inspect.getsourcefile(SocketAddress))),
                                stdout     = stdout,
                                stderr     = subprocess.STDOUT,
                                preexec_fn = os.setsid)
    time.sleep(poll_delay)# give it a chance to start up and try binding to a port i.e. a startup_delay

    port = int(sockA.recv(1024))
    sockA.shutdown(socket.SHUT_RDWR)
    sockA.close()

    if process.poll() is None: # if none then server running correctly and not terminated
        return process.pid, port
    os.system('kill -9 %i >& /dev/null'% process.pid)
    return None, None

##could use either
def stop_server(pid):
    import os, signal
    os.killpg(pid, signal.SIGTERM) # Send the signal to all the process groups
##     import os
##     os.system('kill -9 %i >& /dev/null' % pid)

def kill_server(popen_object):
    import os, signal
    os.killpg(popen_object.pid, signal.SIGKILL) # Send the signal to all the process groups
    popen_object.wait()

    #popen_object.kill()

        
# Interactive running
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
if __name__ == '__main__':
    import inspect, os
    from optparse import OptionParser
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option( "-r", "--return_fd", metavar="FILE_DESC", type="int",
                       action="store", dest="fd",
                       help="File descriptor for socket to return startup port." )
    parser.add_option( "-p", "--port_min", metavar="PORT", type="int",
                       action="store", dest="port_min", default=49000,
                       help="The min port number that the server should "\
                            "try to listen on [default: %default]" )
    parser.add_option( "-x", "--port_max", metavar="PORT", type="int",
                       action="store", dest="port_max", default=65000,
                       help="The max port number that the server should "\
                            "try to listen on [default: %default]" )
    parser.add_option( "-n", "--num_workers", metavar="NUM", type="int",
                       action="store", dest="num_worker_threads", default=5,
                       help="The number of worker thread [default: %default]" )
    parser.add_option( "-t", "--timeout", metavar="TIME", type="int",
                       action="store", dest="command_timeout", default=60,
                       help="Default time in seconds to attempt a command before "\
                            "giving up [default: %default]" )
    parser.add_option( "-c", "--commands", metavar="FILE",
                       action="append", dest="command_files", default=[os.path.join( os.path.dirname(os.path.realpath(inspect.getsourcefile(SocketAddress))),
                                                                                     'DiracCommands.py' )],
                       help="The path to the server's command files. Can repeat this "\
                            "option with as many files as needed [default: %default]" )
    parser.add_option( "-e", "--endDataStr", metavar="END_DATA_STR",
                       action="store", dest="end_data_str", default='###END-DATA-TRANS###',
                       help="The string used to indicate the end of data transmission "\
                            "[default: %default]" )
    parser.add_option( "-s", "--shutdownStr", metavar="SERVER_SHUTDOWN_STR",
                       action="store", dest="server_shutdown_str", default='###SERVER-SHUTDOWN###',
                       help="The string used to indicate that the server should shutdown "\
                            "[default: %default]" )
    (options, args) = parser.parse_args()
    
    ds = DiracServer( port_min            = options.port_min,
                      port_max            = options.port_max,
                      command_files       = set(options.command_files), #unique list
                      command_timeout     = options.command_timeout,
                      num_worker_threads  = options.num_worker_threads,
                      end_data_str        = options.end_data_str,
                      server_shutdown_str = options.server_shutdown_str )
    ds.start(options.fd)
