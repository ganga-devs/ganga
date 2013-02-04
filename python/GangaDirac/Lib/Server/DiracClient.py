#!/usr/bin/env python
from Ganga.GPIDev.Credentials import getCredential
from Ganga.Core import GangaException
from Ganga.Core.GangaThread import GangaThread, GangaThreadPool
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig
import collections, Queue, threading
import os
from GangaDirac.Lib.Utilities.DiracUtilities import getDiracEnv,getDiracCommandIncludes
from GangaDirac.Lib.Utilities.smartsubprocess import runcmd, runcmd_async, CommandOutput

logger = getLogger()
default_timeout  = getConfig('DIRAC')['Timeout']
default_env      = getDiracEnv()
default_includes = getDiracCommandIncludes()
QueueElement  = collections.namedtuple('QueueElement',  ['priority', 'command_input','callback_func','args','kwds'])
CommandInput  = collections.namedtuple('CommandInput',  ['command', 'timeout', 'env', 'cwd'])

class DiracClient(object):
    """
    Client class through which Ganga objects interact with the local DIRAC server.
    """
    
    __slots__ = ['__proxy','__queue']

    def __init__( self, num_worker_threads  = 5):
        self.__proxy = getCredential('GridProxy', '')
        self.__queue = Queue.PriorityQueue()

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

            result = self.execute(*item.command_input)

            if item.callback_func is not None:
                item.callback_func(result, *item.args, **item.kwds)

            #unregster as a working thread bcoz free
            GangaThreadPool.getInstance().delServiceThread(thread)
            self.__queue.task_done()

    def proxyValid(self): return self.__proxy.isValid()
        
    def execute(self, command, timeout=default_timeout, env=default_env, cwd=None):
        """
        Execute a command on the local DIRAC server.

        This function blocks until the server returns.
        """
        if not self.__proxy.isValid(): 
            self.__proxy.create()
            if not self.__proxy.isValid():
                raise GangaException('Can not execute DIRAC API code w/o a valid grid proxy.')
        
        ret = runcmd(default_includes+command, timeout=timeout, env=env, cwd=cwd, usepython=True).stdout
        try:
            ret=eval(ret)
        except: pass
        return ret

    def execute_nonblocking( self, 
                             command, 
                             timeout       = default_timeout,
                             env           = default_env,
                             cwd           = None,
                             priority      = 5,
                             callback_func = None,
                             args          = (),
                             kwds          = {} ):
        """
        Execute a command on the local DIRAC server and pass the output to finalise_code

        This function pust the request on the queue to be executed when a worker becomes available.
        This is therefore a non-blocking function.
        NOTE: if no command is passed i.e. command = None then the function finalise_code is called
        only with args. Otherwise it is called with the result of executing the command on the local
        DIRAC server as the first arg.
        """
        self.__queue.put( QueueElement(priority      = priority,
                                       command_input = CommandInput(command, timeout, env, cwd),
                                       callback_func = callback_func,
                                       args          = args,
                                       kwds          = kwds
                                       ) )

    def get_queue(self):
        """
        Returns the current state of the multiprocess queue that the local DIRAC server is working through.
        """
        return self.__queue.queue[:]
