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
CommandInput  = collections.namedtuple('CommandInput',  ['command', 'timeout', 'env', 'cwd', 'shell'])

class WorkerThreadPool(object):
    """
    Client class through which Ganga objects interact with the local DIRAC server.
    """
    
    __slots__ = ['__proxy','__queue', '__worker_threads']

    def __init__( self,
                  num_worker_threads   = getConfig('DIRAC')['NumWorkerThreads'],
                  worker_thread_prefix = 'Worker_' ):
        self.__proxy = getCredential('GridProxy', '')
        self.__queue = Queue.PriorityQueue()
        self.__worker_threads = []

        for i in range(num_worker_threads):
            t = GangaThread(name = worker_thread_prefix + str(i),
                            auto_register = False,
                            target=self.__worker_thread)
            t._Thread__args=(t,)
            t.name     = worker_thread_prefix + str(i)
            t._command = 'idle'
            t._timeout = 'N/A'
            t.start()
            self.__worker_threads.append(t)

    def proxyValid(self):
        return self.__proxy.isValid()

    def __worker_thread(self, thread):
        """
        Code run by worker threads to allow parallelism in Ganga.

        Can be used for executing non-blocking calls to local DIRAC server
        """
        while not thread.should_stop():
            try:
                item = self.__queue.get(True, 0.05)
            except Queue.Empty: continue #wait 0.05 sec then loop again to give shutdown a chance
            
            result=None
            #regster as a working thread
            GangaThreadPool.getInstance().addServiceThread(thread)
            if item.command_input.command is not None:
                thread._command = item.command_input.command
                thread._timeout = item.command_input.timeout
                result = self.execute(*item.command_input)

            if item.callback_func is not None:
                thread._command = item.callback_func.__name__
                thread._timeout = 'N/A'
                if result is None:
                    try:
                        item.callback_func(*item.args, **item.kwds)
                    except Exception, e:
                        logger.error('Exception raised in %s: %s'%(thread.name,e))
                else:
                    try:
                        item.callback_func(result, *item.args, **item.kwds)
                    except Exception, e:
                        logger.error('Exception raised in %s: %s'%(thread.name,e))

            #unregster as a working thread bcoz free
            GangaThreadPool.getInstance().delServiceThread(thread)
            thread._command = 'idle'
            thread._timeout = 'N/A'
            self.__queue.task_done()
      
    def execute(self, command, timeout=default_timeout, env=default_env, cwd=None, shell=False, pyincludes=default_includes):
        """
        Execute a command on the local DIRAC server.

        This function blocks until the server returns.
        """
        if not self.__proxy.isValid(): 
            self.__proxy.create()
            if not self.__proxy.isValid():
                raise GangaException('Can not execute DIRAC API code w/o a valid grid proxy.')
        
        if shell:
            ret = runcmd(command, timeout=timeout, env=env, cwd=cwd, usepython=False).stdout
        else:
            ret = runcmd(pyincludes+command, timeout=timeout, env=env, cwd=cwd, usepython=True).stdout
        try:
            ret=eval(ret)
        except: pass
        return ret

    def execute_nonblocking( self, 
                             command, 
                             timeout       = default_timeout,
                             env           = default_env,
                             cwd           = None,
                             shell         = False,
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
                                       command_input = CommandInput(command, timeout, env, cwd, shell),
                                       callback_func = callback_func,
                                       args          = args,
                                       kwds          = kwds
                                       ) )

    def get_queue(self):
        """
        Returns the current state of the multiprocess queue that the local DIRAC server is working through.
        """
        return self.__queue.queue[:]


    def worker_status(self):
        return [(w.name, w._command, w._timeout) for w in self.__worker_threads]
