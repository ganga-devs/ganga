#!/usr/bin/env python
from GangaDirac.Lib.Server.DiracServer import SocketAddress
from Ganga.GPIDev.Credentials import getCredential
from Ganga.Core import GangaException
from Ganga.Core.GangaThread import GangaThread, GangaThreadPool
from Ganga.Utility.logging import getLogger
logger = getLogger()
import collections, Queue, threading


QueueElement  = collections.namedtuple('QueueElement',  ['priority', 'command','finalise_code','args','timeout'])

class DiracClient(object):
    """
    Client class through which Ganga objects interact with the local DIRAC server.
    """
    import socket
    import pickle
    
    __slots__ = ['__socket_addr', '__end_data_str', '__server_shutdown_str','__proxy','__queue']#,'__run_workers']#,'__worker_threads']

    def __init__( self,
                  address             = 'localhost',
                  port                = 49000,
                  num_worker_threads  = 5,
                  end_data_str        = '###END-DATA-TRANS###',
                  server_shutdown_str = '###SERVER-SHUTDOWN###' ):
        import atexit
        atexit.register((0,self.shutdown_server))
        #atexit.register((0,self.__stop_workers))#should now be done by gangathread
        self.__socket_addr         = SocketAddress(address = address, port = port)
        self.__end_data_str        = end_data_str
        self.__server_shutdown_str = server_shutdown_str
        self.__proxy = getCredential('GridProxy', '')
        self.__queue = Queue.PriorityQueue()

        ## Setup worker threads
        #self.__run_workers    = True
        #self.__worker_threads = []
        for i in range(num_worker_threads):
            t = GangaThread(name='DiracClient_Thread_%i'%i,
                            auto_register = False,
                            target=self.__worker_thread)
            t._Thread__args=(t,)
            #t.name = 'Worker' + t.name
            t.start()
            #self.__worker_threads.append(t)


    def __worker_thread(self, thread):
        """
        Code run by worker threads to allow parallelism in Ganga.

        Can be used for executing non-blocking calls to local DIRAC server
        """
        while not thread.should_stop():
            try:
                item = self.__queue.get(True, 0.05)
            except Queue.Empty: continue #wait 0.05 sec then loop again to give shutdown a chance

            #regster as a working thread
            GangaThreadPool.getInstance().addServiceThread(thread)

            if item.command is None and item.finalise_code is not None:
                item.finalise_code(*item.args)
                
                #unregster as a working thread bcoz free
                GangaThreadPool.getInstance().delServiceThread(thread)
                self.__queue.task_done()
                continue

            result = self.execute(item.command, item.priority, item.timeout)

            if item.finalise_code is not None:
                item.finalise_code(result, *item.args)

            #unregster as a working thread bcoz free
            GangaThreadPool.getInstance().delServiceThread(thread)
            self.__queue.task_done()

##     def __stop_workers(self):
##         self.__run_workers = False
##         [t.join(1) for t in self.__worker_threads]
            

    def proxyValid(self): return self.__proxy.isValid()

    def __setup_socket(self):
        """
        Sets up the clients socket and connects to the local DIRAC server
        """
        client_socket = DiracClient.socket.socket(DiracClient.socket.AF_INET,
                                                  DiracClient.socket.SOCK_STREAM)
        client_socket.settimeout(10)
        try:
            client_socket.connect(self.__socket_addr)
        except DiracClient.socket.timeout:
            raise GangaException("Timeout connecting to local DiracServer")            
        except:
            raise GangaException("Couldn't connect to %s" % str(self.__socket_addr))
        client_socket.settimeout(None) # turn off timeout now as timeout of commands handled in server
        return client_socket

    def start_interactive(self):
        """
        Run the client interactively.
        """
        if not self.__proxy.isValid(): 
            self.__proxy.create()
            if not self.__proxy.isValid():
                raise GangaException('Can not execute DIRAC API code w/o a valid grid proxy.')
        while True:
            client_socket = self.__setup_socket()
            data = raw_input ( "SEND( TYPE q or Q to Quit):" )
            if (data =='Q' or data =='q'): break
            client_socket.sendall(data + self.__end_data_str)
            result=''
            while True:
                data = client_socket.recv(1024)
                if not data: break
                result += data
                if(result.find(self.__end_data_str) >= 0):
                    print DiracClient.pickle.loads(result.replace(self.__end_data_str,''))
                    break
            client_socket.shutdown(DiracClient.socket.SHUT_RDWR)
            client_socket.close()

        print "INFO: Sending server shutdown signal"
        client_socket.sendall(self.__server_shutdown_str)
        client_socket.shutdown(DiracClient.socket.SHUT_RDWR)
        client_socket.close()

        
    def execute(self, command, priority=0, timeout=60):
        """
        Execute a command on the local DIRAC server.

        This function blocks until the server returns.
        """
        if not self.__proxy.isValid(): 
            self.__proxy.create()
            if not self.__proxy.isValid():
                raise GangaException('Can not execute DIRAC API code w/o a valid grid proxy.')
        client_socket = self.__setup_socket()
        client_socket.sendall(command + ('###PRIORITY=%d###'%priority) + ('###TIMEOUT=%i###'%timeout) + self.__end_data_str)
        result=''
        while True:
            data = client_socket.recv(1024)
            if not data: break
            result += data
            if(result.find(self.__end_data_str) >= 0):
                result = DiracClient.pickle.loads(result.replace(self.__end_data_str,''))
                break
        client_socket.shutdown(DiracClient.socket.SHUT_RDWR)
        client_socket.close()
        if type(result) == type('') and result.find('###TRACEBACK###') >= 0:
            logger.warning('Exception executing DIRAC API code: %s' % result)
            raise GangaException('Exception executing DIRAC API code: %s' % result)
        if type(result) == type('') and result.find('###TIMEOUT###') >= 0:
            logger.warning('Timeout executing DIRAC API code: %s' % result)
            raise GangaException('Timeout executing DIRAC API code: %s' % result)
        return result

    def execute_nonblocking(self, command, priority=5, finalise_code=None, args=(), timeout=60):
        """
        Execute a command on the local DIRAC server and pass the output to finalise_code

        This function pust the request on the queue to be executed when a worker becomes available.
        This is therefore a non-blocking function.
        NOTE: if no command is passed i.e. command = None then the function finalise_code is called
        only with args. Otherwise it is called with the result of executing the command on the local
        DIRAC server as the first arg.
        """
        if command is not None:
            command += self.__end_data_str
        if priority <1:
            priority = 1
        self.__queue.put( QueueElement(priority      = priority,
                                       command       = command,
                                       finalise_code = finalise_code,
                                       args          = args,
                                       timeout       = timeout
                                       ) )

    def get_server_queue(self):
        """
        Returns the current state of the multiprocess queue that the local DIRAC server is working through.
        """
        return self.execute(command='###GET-QUEUE###', priority=1)

    #Note could have this method like above, just call execute, might make code in server nicer too.
    def shutdown_server(self):
        """
        Shutdown the local DIRAC server
        """
        client_socket = self.__setup_socket()
        client_socket.sendall(self.__server_shutdown_str)
        client_socket.shutdown(DiracClient.socket.SHUT_RDWR)
        client_socket.close()
        
         
# Interactive running
#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
if __name__ == '__main__':

    from optparse import OptionParser
    usage = "usage: %prog [options]"
    parser = OptionParser(usage=usage)
    parser.add_option( "-a", "--address", metavar="ADDRESS",
                       action="store", dest="address", default='localhost',
                       help="The string used to identify the server machine "\
                            "[default: %default]" )
    parser.add_option( "-p", "--port", metavar="PORT", type="int",
                       action="store", dest="port", default=49000,
                       help="The port number that the client should "\
                            "try to connect on [default: %default]" )
    parser.add_option( "-n", "--num_workers", metavar="NUM", type="int",
                       action="store", dest="num_worker_threads", default=5,
                       help="The number of worker threads [default: %default]" )
    parser.add_option( "-e", "--endDataStr", metavar="END_DATA_STR",
                       action="store", dest="end_data_str",
                       default='###END-DATA-TRANS###',
                       help="The string used to indicate the end of "\
                            "data transmission [default: %default]" )
    parser.add_option( "-s", "--shutdownStr", metavar="SERVER_SHUTDOWN_STR",
                       action="store", dest="server_shutdown_str",
                       default='###SERVER-SHUTDOWN###',
                       help="The string used to indicate that the server "
                            "should shutdown [default: %default]" )
    (options, args) = parser.parse_args()
    


    dc = DiracClient( address             = options.address,
                      port                = options.port,
                      num_worker_threads  = options.num_worker_threads,
                      end_data_str        = options.end_data_str,
                      server_shutdown_str = options.server_shutdown_str)

    dc.start_interactive()
