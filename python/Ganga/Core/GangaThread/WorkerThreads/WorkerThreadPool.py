#!/usr/bin/env python
import collections, Queue, threading, traceback, signal
import os, pickle, subprocess, types, time, threading
from Ganga.Core.GangaThread                  import GangaThread
from Ganga.Core                              import GangaException
from Ganga.GPIDev.Credentials                import getCredential
from Ganga.Utility.execute                   import execute
from Ganga.Utility.logging                   import getLogger
from Ganga.Utility.Config                    import getConfig

logger = getLogger()
QueueElement   = collections.namedtuple('QueueElement',  ['priority', 'command_input', 'callback_func', 'fallback_func'])
CommandInput   = collections.namedtuple('CommandInput',  ['command', 'timeout', 'env', 'cwd', 'shell', 'python_setup', 'eval_includes', 'update_env'])
FunctionInput  = collections.namedtuple('FunctionInput', ['function', 'args', 'kwargs'])

class WorkerThreadPool(object):
    """
    Client class through which Ganga objects interact with the local DIRAC server.
    """
    __slots__ = ['__queue', '__worker_threads', '_saved_num_worker', '_saved_thread_prefix' ]

    def __init__( self,
                  num_worker_threads   = getConfig('Queues')['NumWorkerThreads'],
                  worker_thread_prefix = 'Worker_' ):
        self.__queue = Queue.PriorityQueue()
        self.__worker_threads = []

        self._saved_num_worker = num_worker_threads
        self._saved_thread_prefix = worker_thread_prefix

        self.__init_worker_threads( self._saved_num_worker, self._saved_thread_prefix )

    def __init_worker_threads(self, num_worker_threads, worker_thread_prefix ):
        if self.__worker_threads:
            logger.info("Threads already started!")
            return

        for i in range(num_worker_threads):
            t = GangaThread(name = worker_thread_prefix + str(i),
                            auto_register = False,
                            target=self.__worker_thread)
            t._Thread__args=(t,)
            t._name    = worker_thread_prefix + str(i)
            t._command = 'idle'
            t._timeout = 'N/A'
            t.start()
            self.__worker_threads.append(t)

    def __worker_thread(self, thread):
        """
        Code run by worker threads to allow parallelism in Ganga.

        Can be used for executing non-blocking calls to local DIRAC server
        """
        # Occasionally when shutting down the Queue import at top has been garbage collected
        # and line "except Queue.Empty: continue" will throw
        # <type 'exceptions.AttributeError'>: 'NoneType' object has no attribute 'Empty'
        # im hoping that importing within the thread will avoid this.
        import Queue

        ## Note can use threading.current_thread to get the thread rather than passing it as an arg
        ## easier to unit test this way though with a dummy thread.
        while not thread.should_stop():
            try:
                item = self.__queue.get(True, 0.05)
            except Queue.Empty: continue #wait 0.05 sec then loop again to give shutdown a chance

            #regster as a working thread
            thread.register()

            if not isinstance(item, QueueElement):
                logger.error("Unrecognised queue element: '%s'" % repr(item))
                logger.error("                  expected: 'QueueElement'")
                self.__queue.task_done()
                thread.unregister()
                continue
             
            if isinstance(item.command_input, FunctionInput):
                thread._command = item.command_input.function.__name__
            elif isinstance(item.command_input, CommandInput):
                thread._command = item.command_input.command
                thread._timeout = item.command_input.timeout
            else:
                logger.error("Unrecognised input command type: '%s'" % repr(item.command_input))
                logger.error("                       expected: ('FunctionInput' or 'CommandInput')")
                self.__queue.task_done()
                thread.unregister()
                continue

            try:
                if isinstance(item.command_input, FunctionInput):
                    result = item.command_input.function(*item.command_input.args, **item.command_input.kwargs)
                else:
                    result = execute(*item.command_input)
            except Exception, e:
                logger.error("Exception raised executing '%s' in Thread '%s':\n%s"%(thread._command, thread.name, traceback.format_exc()))
                if item.fallback_func.function is not None:
                    if isinstance(item.fallback_func, FunctionInput):
                        thread._command = item.fallback_func.function.__name__
                        thread._timeout = 'N/A'
                        try:
                            item.fallback_func.function(e, *item.fallback_func.args, **item.fallback_func.kwargs)
                        except Exception, x:
                            logger.error("Exception raised in fallback function '%s' of Thread '%s':\n%s"%(thread._command, thread.name, traceback.format_exc()))
                    else:
                        logger.error("Unrecognised fallback_func type: '%s'" % repr(item.fallback_func))
                        logger.error("                       expected: 'FunctionInput'")
            else:
                if item.callback_func.function is not None:
                    if isinstance(item.callback_func, FunctionInput):
                        thread._command = item.callback_func.function.__name__
                        thread._timeout = 'N/A'
                        try:
                            item.callback_func.function(result, *item.callback_func.args, **item.callback_func.kwargs)
                        except Exception, e:
                            logger.error("Exception raised in callback_func '%s' of Thread '%s': %s"%(thread._command, thread.name, traceback.format_exc()))
                    else:
                        logger.error("Unrecognised callback_func type: '%s'" % repr(item.callback_func))
                        logger.error("                       expected: 'FunctionInput'") 
            finally:
                # unregister as a working thread bcoz free
                thread._command = 'idle'
                thread._timeout = 'N/A'
                self.__queue.task_done()
                thread.unregister()

    def add_function(self,
                     function, args=(), kwargs={}, priority=5,
                     callback_func=None, callback_args=(), callback_kwargs={},
                     fallback_func=None, fallback_args=(), fallback_kwargs={}):

        if not isinstance(function, types.FunctionType) and not isinstance(function, types.MethodType):
            logger.error('Only a python callable object may be added to the queue using the add_function() method')
            return
        self.__queue.put( QueueElement(priority      = priority,
                                       command_input = FunctionInput(function, args, kwargs),
                                       callback_func = FunctionInput(callback_func, callback_args, callback_kwargs),
                                       fallback_func = FunctionInput(fallback_func, fallback_args, fallback_kwargs)
                                       ) )

    def add_process(self,
                    command, timeout=None, env=None, cwd=None, shell=False,
                    python_setup='', eval_includes=None, update_env=False, priority=5,
                    callback_func=None, callback_args=(), callback_kwargs={},
                    fallback_func=None, fallback_args=(), fallback_kwargs={}):

        if type(command) != str:
            logger.error("Input command must be of type 'string'")
            return
        self.__queue.put( QueueElement(priority      = priority,
                                       command_input = CommandInput(command, timeout, env, cwd, shell, python_setup, eval_includes, update_env),
                                       callback_func = FunctionInput(callback_func, callback_args, callback_kwargs),
                                       fallback_func = FunctionInput(fallback_func, fallback_args, fallback_kwargs)
                                       ) )
    #simple no need for callback
#    def add(self, function, *args, **kwargs):
#        self.add_function(function, args, kwargs)

#    def execute(self, command, timeout=None, env=None, cwd=None, shell=False,
#                python_setup=None, eval_includes=None, update_env=False,priority=5):
#        pass

    ## Legacy methods put here to keep it working while I migrate
    ######################################################################################################
#    def execute(self, command, timeout=getConfig('DIRAC')['Timeout'], env=default_env, cwd=None, shell=False, python_setup=default_includes, eval_includes=None, update_env=False):
#        return execute(command, timeout, env, cwd, shell, python_setup,eval_includes,update_env)

#    def execute_nonblocking( self, 
#                             command,
#                             command_args    = (),
#                             command_kwargs  = {},
#                             timeout         = getConfig('DIRAC')['Timeout'],
#                             env             = default_env,
#                             cwd             = None,
#                             shell           = False,
#                             python_setup    = default_includes,
#                             eval_includes   = None,
#                             update_env      = False,
#                             priority        = 5,
#                             callback_func   = None,
#                             callback_args   = (),
#                             callback_kwargs = {},
#                             fallback_func   = None,
#                             fallback_args   = (),
#                             fallback_kwargs = {}):
#        """
#        Execute a command on the local DIRAC server and pass the output to finalise_code
#
#        This function pust the request on the queue to be executed when a worker becomes available.
#        This is therefore a non-blocking function.
#        NOTE: if no command is passed i.e. command = None then the function finalise_code is called
#        only with args. Otherwise it is called with the result of executing the command on the local
#        DIRAC server as the first arg.
#        """
#        if isinstance(command, types.FunctionType) or isinstance(command, types.MethodType):
#            self.__queue.put( QueueElement(priority      = priority,
#                                           command_input = FunctionInput(command, command_args, command_kwargs),
#                                           callback_func = FunctionInput(callback_func, callback_args, callback_kwargs),
#                                           fallback_func = FunctionInput(fallback_func, fallback_args, fallback_kwargs)
#                                           ) )
#        else:
#            self.__queue.put( QueueElement(priority      = priority,
#                                           command_input = CommandInput(command, timeout, env, cwd, shell, python_setup, eval_includes, update_env),
#                                           callback_func = FunctionInput(callback_func, callback_args, callback_kwargs),
#                                           fallback_func = FunctionInput(fallback_func, fallback_args, fallback_kwargs)
#                                           ) )
#
#    ######################################################################################################
    def map(function, *iterables):
        if not isinstance(function, types.FunctionType) \
                and not isinstance(function, types.MethodType):
            raise Exception('must be a function')
        for args in zip(*iterables):
            self.__queue.put( QueueElement(priority      = 5,
                                           command_input = FunctionInput(function, args, {})
                                           ) )
            
    def clear_queue(self):
        """
        Purges the thread pools queue.
        """
        self.__queue.queue=[]

    def get_queue(self):
        """
        Returns the current state of the multiprocess queue that the local DIRAC server is working through.
        """
        return self.__queue.queue[:]


    def worker_status(self):
        """
        Returns a informatative tuple containing the threads name, current command it's working on and the timeout for that command.
        """
        return [(w._name, w._command, w._timeout) for w in self.__worker_threads]

    def _stop_worker_threads(self):
        for w in self.__worker_threads:
            w.stop()
        del self.__worker_threads[:]
        return

    def _start_worker_threads(self):
        self.__init_worker_threads( self._saved_num_worker, self._saved_thread_prefix )
        return

###################################################################
