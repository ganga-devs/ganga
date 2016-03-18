from __future__ import print_function
import collections
from Ganga.Core.GangaThread.WorkerThreads.WorkerThreadPool import WorkerThreadPool
from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger
from Ganga.Utility.ColourText import getColour
from Ganga.GPIDev.Base.Proxy import getName
logger = getLogger()

_user_threadpool = None
_monitoring_threadpool = None


class ThreadPoolQueueMonitor(object):

    '''
    This class displays the user and monitor thread pools and associated queues

    The number of worker threads in the pool is initialized by
    the getConfig('Queues')['NumWorkerThreads'] config option.
    '''

    def __init__(self, user_threadpool=None, monitoring_threadpool=None):

        if user_threadpool is None:
            user_threadpool = WorkerThreadPool(worker_thread_prefix="User_Worker_")
        if monitoring_threadpool is None:
            monitoring_threadpool = WorkerThreadPool(worker_thread_prefix="Ganga_Worker_")

        global _user_threadpool
        global _monitoring_threadpool
        self._user_threadpool = _user_threadpool
        self._monitoring_threadpool = _monitoring_threadpool

        if user_threadpool != None:
            if self._user_threadpool is not None:
                self._user_threadpool.clear_queue()
                self._user_threadpool._stop_worker_threads()
                del self._user_threadpool
                del _user_threadpool
            self._user_threadpool = user_threadpool
        if monitoring_threadpool != None:
            if self._monitoring_threadpool is not None:
                self._monitoring_threadpool.clear_queue()
                self._monitoring_threadpool._stop_worker_threads()
                del self._monitoring_threadpool
                del _monitoring_threadpool
            self._monitoring_threadpool = monitoring_threadpool

        _user_threadpool = self._user_threadpool
        _monitoring_threadpool = self._monitoring_threadpool

        self._frozen = False
        self._shutdown = False

    def _display_element(self, item):
        if hasattr(item, 'name') and item.name != None:
            return item.name
        elif type(item.command_input[0]) != str:
            return getName(item.command_input[0])
        else:
            return item.command_input[0]

    def _display(self):
        '''Return the current status of the thread pools and queues.'''
        output = ''
        output += '{0:^67} | {1:^50}\n'.format('Ganga user threads:', 'Ganga monitoring threads:')
        output += '{0:^67} | {1:^50}\n'.format('------------------', '------------------------')
        output += '{0:<26} {1:<26} {2:<13} | {0:<26} {1:<28} {2:<10}\n'.format('Name', 'Command', 'Timeout')
        output += '{0:<26} {1:<26} {2:<13} | {0:<26} {1:<28} {2:<10}\n'.format('----', '-------', '-------')
        for u, m in zip(self._user_threadpool.worker_status(),
                        self._monitoring_threadpool.worker_status()):
            # name has extra spaces as colour characters are invisible but
            # still count
            if u[0] is not None:
                # User task has no ganga name
                name_user = getColour('fg.red') + str(u[0]) + getColour('fg.normal')
            else:
                # User task has a ganga name
                name_user = getColour('fg.red') + str(u[1]) + getColour('fg.normal')
            name_monitor = getColour('fg.red') + str(m[0]) + getColour('fg.normal')
            if u[1] == 'idle':
                name_user = name_user.replace(getColour('fg.red'), getColour('fg.green'))
            if m[1] == 'idle':
                name_monitor = name_monitor.replace(getColour('fg.red'), getColour('fg.green'))
            output += '{0:<35} {1:<26} {2:<15} | {3:<35} {4:<28} {5:<10}\n'.format(
                name_user, u[1][:30].replace("\n", "\\n"), u[2], name_monitor, m[1][:30].replace("\n", "\\n"), m[2])

        output += '\n'
        output += "Ganga user queue:\n"
        output += "----------------\n"
        output += str([self._display_element(elem) for elem in self._user_threadpool.get_queue()])
        output += '\n'
        output += "Ganga monitoring queue:\n"
        output += "----------------------\n"
        output += str([self._display_element(elem) for elem in self._monitoring_threadpool.get_queue()])
        return output

    def _repr_pretty_(self, p, cycle):
        if cycle:
            p.text('tasks...')
            return
        p.text(self._display())

    def __shouldWaitonShutdown(self):
        from Ganga.Core import getCurrentShutdownPolicy

        if getCurrentShutdownPolicy() == 'batch':
            return True
        else:
            return False

    def purge(self, force=None):
        """
        Purge the Ganga user thread pool's queue
        """
        if force is None:
            force = self.__shouldWaitonShutdown()

        _user_queue = [i for i in self._user_threadpool.get_queue()]
        queue_size = len(_user_queue)
        _actually_purge = False
        if force == True:
            _actually_purge = True
        if queue_size > 0 and not force:
            keyin = None
            while keyin == None:
                print("User queue contains unfinished tasks:")
                print(str([self._display_element(i) for i in _user_queue]))
                keyin = raw_input("Do you want to Purge the user queue ([y]/n): ")
                if keyin in ['y', '']:
                    _actually_purge = True
                elif keyin == 'n':
                    _actually_purge = False
                else:
                    print("(y/n) please!")
                    keyin = None
        if _actually_purge:
            self._user_threadpool.clear_queue()

    def _purge_all(self, force=None):
        """
        Purge ALL of the Ganga user AND Worker thread queues!
        """

        if force is None:
            force = self.__shouldWaitonShutdown()

        self.purge(force)

        _monitor_queue = [i for i in self._monitoring_threadpool.get_queue()]

        queue_size = len(_monitor_queue)
        _actually_purge = False
        if force == True:
            _actually_purge = True
        if queue_size > 0 and not force:
            keyin = None
            while keyin == None:
                print("Monitoring queue contains unfinished tasks:")
                print(str([self._display_element(i) for i in _monitor_queue]))
                keyin = raw_input("Do you want to Purge the monitoring queue ([y]/n): ")
                if keyin in ['y', '']:
                    _actually_purge = True
                elif keyin == 'n':
                    _actually_purge = False
                else:
                    print("(y/n) please")
                    keyin = None
        if _actually_purge:
            self._monitoring_threadpool.clear_queue()

    def add(self, worker_code, args=(), kwargs={}, priority=5):
        """
        Run any python callable object asynchronously through the user thread pool

        Code added to the queue will remain there until a free worker becomes
        available and picks it from the list. At this point the code will run
        asynchronously leaving the interpreter prompt unblocked and ready for user
        input.

        A simple example might be to download user data which would in the
        past block the prompt. Here I use job #72 and '/home/alex' arbitrarily
        for more info about the getOutputData command type help(Dirac.getOutputData).

        In[0]: queues.add(jobs(72).backend.getOutputData, ('/home/alex',))

        Note:
        ----

        Unlike with the queues.addProcess (where the overhead in starting up the processes 
        take a few seconds) code executes immediately when reported using monitoring
        command queues

        args:
        ----
                   worker_code = Any python callable object to run on thread 
                   args        = Any args for the callable object given here
                                 as a tuple
                   kwargs      = Any kwargs for the callable object given here
                                 as a dict
                   priority    = The thread queuing system is a priority
                                 queue with lower number = higher priority.
                                 This then should be an int normally 0-9
        """
        if not isinstance(worker_code, collections.Callable):
            logger.error('Only python callable objects can be added to the queue using queues.add()')
            logger.error('Did you perhaps try to add the return value of the function/method rather than the function/method itself')
            logger.error('e.g. Incorrect:     queues.add(myfunc()) *NOTE the brackets*')
            logger.error('e.g. Correct  :     queues.add(myfunc)')
            return
        self._user_threadpool.add_function(worker_code,
                                           args=args,
                                           kwargs=kwargs,
                                           priority=priority)

    def _addSystem(self, worker_code, args=(), kwargs={}, priority=5):

        if not isinstance(worker_code, collections.Callable):
            logger.error("Error Adding internal task!! please report this to the Ganga developers!")
            return

        if self._frozen is True:
            if not self._shutdown:
                logger.warning("Queue System is frozen not adding any more System processes!")
            return

        self._monitoring_threadpool.add_function(worker_code,
                                                 args=args,
                                                 kwargs=kwargs,
                                                 priority=priority)

    def addProcess(self,
                   command,
                   timeout=getConfig('Queues')['Timeout'],
                   env=None,
                   cwd=None,
                   shell=False,
                   eval_includes=None,
                   update_env=False,
                   priority=5,
                   callback_func=None,
                   callback_args=(),
                   callback_kwargs={},
                   fallback_func=None,
                   fallback_args=(),
                   fallback_kwargs={}):
        """
        Run a command asynchronously in a new process monitored by the user thread pool.

        Note:
        ----

        There will be no return value from this method as it runs the code asynchronously.
        stdout will go through the callback_func so if you want it displayed then use a printing function

        In[2]: def printer(x): print x
        In[0]: queues.addProcess('print 123', callback_func=printer)
        123

        Note also that a process may take a few seconds to start up unlike the thread version queues.add

        args:
        ----
                   command         = The command to run as a string 
                   timeout         = timeout for the command as int or None
                   env             = environment to run command in as dict or None
                   cwd             = working dir to run command in as string
                   shell           = True/False whether to interpret the
                                     command as a python or shell command
                   eval_includes   = This is a string that will be exec'ed before
                                     trying to eval the stdout. This allows for the
                                     the user to import certain libs before attempting
                                     to parse the output.
                   update_env      = Boolean value determining if the value of env should
                                     be updated with the environment after running the
                                     command.
                   priority        = The thread queuing system is a priority
                                     queue with lower number = higher priority.
                                     This then should be an int normally 0-9
                   callback_func   = Any python callable object. This is called
                                     once the command has finished running (or 
                                     timed out) and must take at least one arg.
                                     This first arg will be the stdout of the 
                                     executed command. This arg will be the
                                     result of unpickling stdout, falling back to 
                                     eval(stdout) such that '{}' will
                                     become a dict. Fall back to str representation
                                     if the eval fails.
                   callback_args   = Any additional args to the callback_func 
                                     are specified here as a tuple
                   callback_kwargs = kwargs for the callback_func are given here
                                     as a dict.
                   fallback_func   = Any python callable object. This is called
                                     if the command execution throws an exception.
                                     The function must take at least one arg that
                                     will be the exception that was thrown.
                   fallback_args   = Any additional args to the fallback_func 
                                     are specified here as a tuple
                   fallback_kwargs = kwargs for the fallback_func are given here
                                     as a dict.
        """
        if type(command) != type(''):
            logger.error("Input command must be of type 'string'")
            return

        if self._frozen is True:
            if not self._shutdown:
                logger.warning("Queues system is frozen. Not adding any more processes!")
            return

        self._user_threadpool.add_process(command,
                                          timeout=timeout,
                                          env=env,
                                          cwd=cwd,
                                          shell=shell,
                                          eval_includes=eval_includes,
                                          update_env=update_env,
                                          priority=priority,
                                          callback_func=callback_func,
                                          callback_args=callback_args,
                                          callback_kwargs=callback_kwargs,
                                          fallback_func=fallback_func,
                                          fallback_args=fallback_args,
                                          fallback_kwargs=fallback_kwargs)

    def threadStatus(self):
        statuses = []
        for t in self._user_threadpool.worker_status():
            if t[1] != "idle":
                statuses.append(t[0])
        for t in self._monitoring_threadpool.worker_status():
            if t[1] != "idle":
                statuses.append(t[0])
        return statuses

    def totalNumUserThreads(self):
        """Return the total number of user threads, both running and queued"""
        num = 0
        for t in self._user_threadpool.worker_status():
            if t[1] != "idle":
                num += 1

        return num + len(self._user_threadpool.get_queue())

    def totalNumIntThreads(self):
        num = 0
        for t in self._monitoring_threadpool.worker_status():
            if t[1] != "idle":
                num += 1

        return num + len(self._user_threadpool.get_queue())

    def totalNumAllThreads(self):
        """Return the total number of ALL user and worker threads currently running and queued"""
        num = 0
        num = num + self.totalNumUserThreads()
        num = num + self.totalNumIntThreads()
        return num

    def freeze(self):
        self._frozen = True
        self._user_threadpool._frozen = True
        self._monitoring_threadpool._frozen = True

    def unfreeze(self):
        self._frozen = False
        self._user_threadpool._frozen = False
        self._monitoring_threadpool._frozen = False

    def _stop_all_threads(self, shutdown=False):
        self._shutdown = shutdown
        self._user_threadpool._stop_worker_threads(shutdown)
        self._monitoring_threadpool._stop_worker_threads(shutdown)
        return

    def _start_all_threads(self):
        self._user_threadpool._start_worker_threads()
        self._monitoring_threadpool._start_worker_threads()
        return

