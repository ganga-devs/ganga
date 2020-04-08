#!/usr/bin/env python
import queue
import traceback
import threading
import collections
from GangaCore.Core.exceptions import GangaException, GangaTypeError
from GangaCore.Core.GangaThread import GangaThread
from GangaCore.Utility.execute import execute
from GangaCore.Utility.logging import getLogger
from GangaCore.Utility.Config import getConfig
from GangaCore.GPIDev.Base.Proxy import getName

from collections import namedtuple

timeout = getConfig('Queues')['ShutDownTimeout']
timeout = 0.1 if timeout==None else timeout

logger = getLogger()
QueueElement = namedtuple('QueueElement',  ['priority', 'command_input', 'callback_func', 'fallback_func', 'name'])
CommandInput = namedtuple('CommandInput',  ['command', 'timeout', 'env', 'cwd', 'shell', 'python_setup', 'eval_includes', 'update_env'])
class FunctionInput(namedtuple('FunctionInput', ['function', 'args', 'kwargs'])):
    def __gt__(self, other):
        pass
    def __lt__(self, other):
        pass



class WorkerThreadPool(object):

    """
    Client class through which Ganga objects interact with the local DIRAC server.
    """
    __slots__ = ['__queue', '__worker_threads',
                 '_saved_num_worker', '_saved_thread_prefix', '_frozen', '_shutdown']

    def __init__(self, num_worker_threads=None, worker_thread_prefix='Worker_'):
        if num_worker_threads is None:
            num_worker_threads=getConfig('Queues')['NumWorkerThreads']
        self.__queue = queue.PriorityQueue()
        self.__worker_threads = []

        self._saved_num_worker = num_worker_threads
        self._saved_thread_prefix = worker_thread_prefix

        self.__init_worker_threads(self._saved_num_worker, self._saved_thread_prefix)

        self._frozen = False
        self._shutdown = False

    def __init_worker_threads(self, num_worker_threads, worker_thread_prefix):
        if len(self.__worker_threads) > 0:
            logger.warning("Threads already started!")
            for i in self.__worker_threads:
                logger.info(
                    "Worker Thread: %s is already running!" % i.gangaName)
            return

        for i in range(num_worker_threads):
            t = GangaThread(name=worker_thread_prefix + str(i),
                            auto_register=False,
                            target=self.__worker_thread)
            t._name = worker_thread_prefix + str(i)
            t._command = 'idle'
            t._timeout = 'N/A'
            t.start()
            self.__worker_threads.append(t)

    def __worker_thread(self):
        """
        Code run by worker threads to allow parallelism in GangaCore.

        Can be used for executing non-blocking calls to local DIRAC server
        """
        thread = threading.current_thread()
        # Occasionally when shutting down the Queue import at top has been garbage collected
        # and line "except Queue.Empty: continue" will throw
        # <type 'exceptions.AttributeError'>: 'NoneType' object has no attribute 'Empty'
        # im hoping that importing within the thread will avoid this.
        import queue

        oldname = thread.gangaName

        # Note can use threading.current_thread to get the thread rather than passing it as an arg
        # easier to unit test this way though with a dummy thread.
        while not thread.should_stop():
            try:
                item = self.__queue.get(True,timeout)
            except queue.Empty:
                # wait 'timeout' sec then loop again to give shutdown a chance
                continue

            # regster as a working thread
            if isinstance(item, QueueElement):
                oldname = thread.gangaName
                thread.gangaName = item.name

            thread.register()

            if not isinstance(item, QueueElement):
                logger.error("Unrecognised queue element: '%s'" % repr(item))
                logger.error("                  expected: 'QueueElement'")
                self.__queue.task_done()
                thread.unregister()
                continue

            if isinstance(item.command_input, FunctionInput):
                thread._command = getName(item.command_input.function)
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
                    these_args = item.command_input.args
                    if isinstance(these_args, str):
                        these_args = (these_args, )
                    result = item.command_input.function(*these_args, **item.command_input.kwargs)
                else:
                    result = execute(*item.command_input)
            except Exception as e:
                if issubclass(type(e), GangaException):
                    logger.error("%s" % e)
                else:
                    logger.error("Exception raised executing '%s' in Thread '%s':\n%s" % (thread._command, thread.gangaName, traceback.format_exc()))
                    if item.fallback_func.function is not None:
                        if isinstance(item.fallback_func, FunctionInput):
                            thread._command = getName(item.fallback_func.function)
                            thread._timeout = 'N/A'
                            try:
                                item.fallback_func.function(e, *item.fallback_func.args, **item.fallback_func.kwargs)
                            except Exception as x:
                                if not issubclass(type(e), GangaException):
                                    logger.error("Exception raised in fallback function '%s' of Thread '%s':\n%s" % (thread._command, thread.gangaName, traceback.format_exc()))
                                else:
                                    logger.error("%s" % x)
                        else:
                            logger.error("Unrecognised fallback_func type: '%s'" % repr(item.fallback_func))
                            logger.error("                       expected: 'FunctionInput'")
            else:
                if item.callback_func.function is not None:
                    if isinstance(item.callback_func, FunctionInput):
                        thread._command = getName(item.callback_func.function)
                        thread._timeout = 'N/A'
                        try:
                            item.callback_func.function(
                                result, *item.callback_func.args, **item.callback_func.kwargs)
                        except Exception as e:
                            if not issubclass(type(e), GangaException):
                                logger.error("Exception raised in callback_func '%s' of Thread '%s': %s" % (
                                    thread._command, thread.gangaName, traceback.format_exc()))
                            else:
                                logger.error("%s" % e)
                    else:
                        logger.error("Unrecognised callback_func type: '%s'" % repr(item.callback_func))
                        logger.error("                       expected: 'FunctionInput'")
            finally:
                # unregister as a working thread bcoz free
                thread._command = 'idle'
                thread._timeout = 'N/A'
                self.__queue.task_done()
                thread.unregister()

            thread.gangaName = oldname

    def add_function(self,
                     function, args=(), kwargs={}, priority=5,
                     callback_func=None, callback_args=(), callback_kwargs={},
                     fallback_func=None, fallback_args=(), fallback_kwargs={},
                     name=None):

        if not isinstance(function, collections.abc.Callable):
            logger.error('Only a python callable object may be added to the queue using the add_function() method')
            return
        if self.isfrozen() is True:
            if not self._shutdown:
                logger.warning("Cannot Add Process as Queue is frozen!")
            return
        self.__queue.put(QueueElement(priority=priority,
                                      command_input=FunctionInput(
                                          function, args, kwargs),
                                      callback_func=FunctionInput(
                                          callback_func, callback_args, callback_kwargs),
                                      fallback_func=FunctionInput(fallback_func, fallback_args, fallback_kwargs), name=name
                                      ))

    def add_process(self,
                    command, timeout=None, env=None, cwd=None, shell=False,
                    python_setup='', eval_includes=None, update_env=False, priority=5,
                    callback_func=None, callback_args=(), callback_kwargs={},
                    fallback_func=None, fallback_args=(), fallback_kwargs={},
                    name=None):

        if not isinstance(command, str):
            logger.error("Input command must be of type 'string'")
            return
        if self.isfrozen() is True:
            if self._shutdown:
                logger.warning("Cannot Add Process as Queue is frozen!")
            return
        self.__queue.put(QueueElement(priority=priority,
                                      command_input=CommandInput(
                                          command, timeout, env, cwd, shell, python_setup, eval_includes, update_env),
                                      callback_func=FunctionInput(
                                          callback_func, callback_args, callback_kwargs),
                                      fallback_func=FunctionInput(fallback_func, fallback_args, fallback_kwargs), name=name
                                      ))

    def map(self, function, *iterables):
        if not isinstance(function, collections.abc.Callable):
            raise GangaTypeError('must be a function')
        if self.isfrozen() is True:
            logger.error("Cannot map a Function as Queue is frozen!")
        for args in zip(*iterables):
            self.__queue.put(QueueElement(priority=5,
                                          command_input=FunctionInput(
                                              function, args, {})
                                          ))

    def clear_queue(self):
        """
        Purges the thread pools queue.
        """
        self.__queue.queue = []

    def get_queue(self):
        """
        Returns the current state of the multiprocess queue that the local DIRAC server is working through.
        """
        return self.__queue.queue[:]

    def worker_status(self):
        """
        Returns a informatative tuple containing the threads name, current command it's working on and the timeout for that command.
        """
        return [(w.gangaName, w._command, w._timeout) for w in self.__worker_threads]

    def threads_matching(self, name_str):
        """
        Returns a list of all threads matching the given string at the start of their name
        Args:
            name_str (str): String to be used in the starts_with comparison of the worker_threads
        """
        return [w for w in self.__worker_threads if w.gangaName.startswith(name_str)]

    def isfrozen(self):
        return self._frozen

    def freeze(self):
        self._frozen = True

    def unfreeze(self):
        self._frozen = False

    def _stop_worker_threads(self, shutdown=False):
        self._shutdown = shutdown
        for w in self.__worker_threads:
            w.stop()
            w.join()
            # FIXME NEED TO CALL AN OPTIONAL CLEANUP FUCNTION HERE IF THREAD IS STOPPED
            # w.unregister()
            #del w
        self.__worker_threads = []
        return

    def _start_worker_threads(self):
        if len(self.__worker_threads) > 0:
            self._stop_worker_threads()

        self.__init_worker_threads(
            self._saved_num_worker, self._saved_thread_prefix)
        return

###################################################################

