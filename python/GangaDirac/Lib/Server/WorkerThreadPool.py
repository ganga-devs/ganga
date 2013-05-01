#!/usr/bin/env python
from Ganga.GPIDev.Credentials import getCredential
from Ganga.Core import GangaException
from Ganga.Core.GangaThread import GangaThread, GangaThreadPool
from Ganga.Utility.logging import getLogger
from Ganga.Utility.Config import getConfig
import collections, Queue, threading, traceback
import os, pickle, subprocess, types, time, threading
from GangaDirac.Lib.Utilities.DiracUtilities import getDiracEnv,getDiracCommandIncludes
#from GangaDirac.Lib.Utilities.smartsubprocess import runcmd, runcmd_async, CommandOutput

logger = getLogger()
#default_timeout  = getConfig('DIRAC')['Timeout']
default_env      = getDiracEnv()
default_includes = getDiracCommandIncludes()
QueueElement  = collections.namedtuple('QueueElement',  ['priority', 'command_input','callback_func','fallback_func'])
CommandInput  = collections.namedtuple('CommandInput',  ['command', 'timeout', 'env', 'cwd', 'shell', 'python_setup', 'eval_includes'])
FunctionInput  = collections.namedtuple('FunctionInput',  ['function', 'args', 'kwargs'])

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
            t._name    = worker_thread_prefix + str(i)
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
        # Occasionally when shutting down the Queue import at top has been garbage collected
        # and line "except Queue.Empty: continue" will throw
        # <type 'exceptions.AttributeError'>: 'NoneType' object has no attribute 'Empty'
        # im hoping that importing within the thread will avoid this.
        import Queue

        while not thread.should_stop():
            try:
                item = self.__queue.get(True, 0.05)
            except Queue.Empty: continue #wait 0.05 sec then loop again to give shutdown a chance

            #regster as a working thread
            #thread.register()
            GangaThreadPool.getInstance().addServiceThread(thread)

            if isinstance(item.command_input, FunctionInput):
                thread._command = item.command_input.function.__name__
            elif isinstance(item.command_input, CommandInput):
                thread._command = item.command_input.command
                thread._timeout = item.command_input.timeout
            else:
                logger.error("Unrecognised input command type")

            try:
                if isinstance(item.command_input, FunctionInput):
                    result = item.command_input.function(*item.command_input.args, **item.command_input.kwargs)
                else:
                    result = self.execute(*item.command_input)
            except Exception, e:
                logger.error("Exception raised executing '%s' in Thread '%s':\n%s"%(thread._command, thread.name, traceback.format_exc()))
                if isinstance(item.fallback_func, FunctionInput) and item.fallback_func.function is not None:
                    thread._command = item.fallback_func.function.__name__
                    thread._timeout = 'N/A'
                    try:
                        item.fallback_func.function(e, *item.fallback_func.args, **item.fallback_func.kwargs)
                    except Exception, x:
                        logger.error("Exception raised in fallback function '%s' of Thread '%s':\n%s"%(thread._command, thread.name, traceback.format_exc()))
            else:
                if isinstance(item.callback_func, FunctionInput) and item.callback_func.function is not None:
                    thread._command = item.callback_func.function.__name__
                    thread._timeout = 'N/A'
                    try:
                        item.callback_func.function(result, *item.callback_func.args, **item.callback_func.kwargs)
                    except Exception, e:
                        logger.error("Exception raised in callback_func '%s' of Thread '%s': %s"%(thread._command, thread.name, traceback.format_exc()))
            finally:
                # unregister as a working thread bcoz free
                thread._command = 'idle'
                thread._timeout = 'N/A'
                self.__queue.task_done()
                #thread.unregister()
                GangaThreadPool.getInstance().delServiceThread(thread)

    def __timeout_func(self, process, timeout, command_done, timed_out):
        import time
        start = time.time()
        while time.time()-start < timeout:
            if command_done.isSet(): break
            time.sleep(0.5)
        else:
            if process.returncode is None:
                timed_out.set()
                try:
                    process.kill()
                except Exception, e:
                    logger.error("Exception trying to kill process: %s"%e)
 
    def execute(self, command, timeout=getConfig('DIRAC')['Timeout'], env=default_env, cwd=None, shell=False, python_setup=default_includes, eval_includes=None):
        """
        Execute a command on the local DIRAC server.

        This function blocks until the server returns.
        """
        if not self.__proxy.isValid(): 
            self.__proxy.create()
            if not self.__proxy.isValid():
                raise GangaException('Can not execute DIRAC API code w/o a valid grid proxy.')
        
        if shell:
#            stream_command = '`cat<&0`' # didn't quite work for complicated ones using " marks, possibily due to ``
            stream_command = 'cat<&0 | sh'
        else:
            stream_command = 'python -'
            command = python_setup + command

        p=subprocess.Popen(stream_command, shell=True, env=env, cwd=cwd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
## This has been moved into a separate "private method to alow exec in function"
#        def timeout_func(process, timeout, command_done, timed_out):
#            import time
#            start = time.time()
#            while time.time()-start < timeout:
#                if command_done.isSet(): break
#                time.sleep(0.5)
#            else:
#                if process.returncode is None:
#                    timed_out.set()
#                    try:
#                        process.kill()
#                    except Exception, e:
#                        logger.error("Exception trying to kill process: %s"%e)

        command_done = threading.Event()
        timed_out    = threading.Event()
        if timeout is not None:
            t=threading.Thread(target=self.__timeout_func,args=(p, timeout, command_done, timed_out))
            t.deamon=True
            t.start()

## Unfortunately using poll() is buggy as if the user code (e.g. lhcbdirac.getDataset called from BKQuery)
## uses os.wait() or something like it then poll will always return None. Need thread solution above
## a shame as messy.
#        if timeout is not None:
#            start_time = time.time()
#            while p.poll() is None:
#                if time.time()-start_time >=timeout:
#                    p.kill()
#                    if not shell: os.close(outread)
#                    p.communicate()
#                    #out='Command timed out!'
#                    return 'Command timed out!'
##                    break
#                time.sleep(0.5)
#        print "Command =",command
        stdout, stderr = p.communicate(command)
        command_done.set()
        if timed_out.isSet():
            return 'Command timed out!'

        try:
            stdout = pickle.loads(stdout)
        except:
            local_ns = {}
            if type(eval_includes) is str:
                exec(eval_includes, {}, local_ns)
            try:
                stdout = eval(stdout, {}, local_ns)
            except: pass
        return stdout


    def execute_nonblocking( self, 
                             command,
                             command_args        = (),
                             command_kwargs      = {},
                             timeout         = getConfig('DIRAC')['Timeout'],
                             env             = default_env,
                             cwd             = None,
                             shell           = False,
                             python_setup    = default_includes,
                             eval_includes   = None,
                             priority        = 5,
                             callback_func   = None,
                             callback_args   = (),
                             callback_kwargs = {},
                             fallback_func   = None,
                             fallback_args   = (),
                             fallback_kwargs = {}):
        """
        Execute a command on the local DIRAC server and pass the output to finalise_code

        This function pust the request on the queue to be executed when a worker becomes available.
        This is therefore a non-blocking function.
        NOTE: if no command is passed i.e. command = None then the function finalise_code is called
        only with args. Otherwise it is called with the result of executing the command on the local
        DIRAC server as the first arg.
        """
        if isinstance(command, types.FunctionType) or isinstance(command, types.MethodType):
            self.__queue.put( QueueElement(priority      = priority,
                                           command_input = FunctionInput(command, command_args, command_kwargs),
                                           callback_func = FunctionInput(callback_func, callback_args, callback_kwargs),
                                           fallback_func = FunctionInput(fallback_func, fallback_args, fallback_kwargs)
                                           ) )
        else:
            self.__queue.put( QueueElement(priority      = priority,
                                           command_input = CommandInput(command, timeout, env, cwd, shell, python_setup, eval_includes),
                                           callback_func = FunctionInput(callback_func, callback_args, callback_kwargs),
                                           fallback_func = FunctionInput(fallback_func, fallback_args, fallback_kwargs)
                                           ) )

    def clear_queue(self):
        self.__queue.queue=[]

    def get_queue(self):
        """
        Returns the current state of the multiprocess queue that the local DIRAC server is working through.
        """
        return self.__queue.queue[:]


    def worker_status(self):
        return [(w._name, w._command, w._timeout) for w in self.__worker_threads]
