import types
#from GangaDirac.Lib.Backends.DiracBase       import dirac_ganga_server, dirac_monitoring_server
#from GangaDirac.BOOT                         import dirac_ganga_server, dirac_monitoring_server
from GangaDirac.Lib.Utilities.DiracUtilities import getDiracEnv
from Ganga.Utility.Config                    import getConfig
from Ganga.Utility.logging                   import getLogger
from Ganga.Utility.ColourText                import getColour
logger = getLogger()


class ThreadPoolQueueMonitor(object):
    '''
    This class displays the user and monitor thread pools and associated queues

    The number of worker threads in the pool is controlled by
    the getConfig('DIRAC')['NumWorkerThreads'] config option.
    '''

    def _display(self, i):
        '''Return the current status of the thread pools and queues.'''
        from GangaDirac.BOOT import dirac_ganga_server, dirac_monitoring_server
        output=''
        output+= '{0:^55} | {1:^50}\n'.format('Ganga user threads:','Ganga monitoring threads:')
        output+= '{0:^55} | {1:^50}\n'.format('------------------', '------------------------')
        output+= '{0:<10} {1:<33} {2:<10} | {0:<10} {1:<33} {2:<10}\n'.format('Name', 'Command', 'Timeout')
        output+= '{0:<10} {1:<33} {2:<10} | {0:<10} {1:<33} {2:<10}\n'.format('----', '-------', '-------')
        for u, m in zip( dirac_ganga_server.worker_status(),
                         dirac_monitoring_server.worker_status() ):
            # name has extra spaces as colour characters are invisible but still count
            name_user    = getColour('fg.red') + u[0] + getColour('fg.normal')
            name_monitor = getColour('fg.red') + m[0] + getColour('fg.normal')
            if u[1] == 'idle':
                name_user = name_user.replace(getColour('fg.red'), getColour('fg.green'))
            if m[1] == 'idle':
                name_monitor = name_monitor.replace(getColour('fg.red'), getColour('fg.green'))
            output+= '{0:<21} {1:<33} {2:<10} | {3:<21} {4:<33} {5:<10}\n'.format(name_user, u[1][:30].replace("\n","\\n"), u[2], name_monitor, m[1][:30].replace("\n","\\n"), m[2])

        def display_element(item):
            if type(item.command_input[0]) != str:
                return item.command_input[0].__name__
            return item.command_input[0]
        output+= '\n'
        output+= "Ganga user queue:\n"
        output+= "----------------\n"
        output+= str([display_element(i) for i in dirac_ganga_server.get_queue()])
        output+= '\n'
        output+= "Ganga monitoring queue:\n"
        output+= "----------------------\n"
        output+= str([display_element(i) for i in dirac_monitoring_server.get_queue()])
        return output

    def purge(self):
        """
        Purge the Ganga user thread pool's queue
        """
        from GangaDirac.BOOT import dirac_ganga_server
        dirac_ganga_server.clear_queue()

    def add(self, worker_code, args=(), kwargs={}, priority = 5):
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
        from GangaDirac.BOOT import dirac_ganga_server
        if not isinstance(worker_code, types.FunctionType) and not isinstance(worker_code, types.MethodType):
            logger.error('Only python callable objects can be added to the queue using queues.add()')
            logger.error('Did you perhaps try to add the return value of the function/method rather than the function/method itself')
            logger.error('e.g. Incorrect:     queues.add(myfunc()) *NOTE the brackets*')
            logger.error('e.g. Correct  :     queues.add(myfunc)')
            return
        dirac_ganga_server.execute_nonblocking(worker_code,
                                               command_args   = args,
                                               command_kwargs = kwargs,
                                               priority       = priority)

    def addProcess(self, 
                   command, 
                   timeout         = getConfig('DIRAC')['Timeout'],
                   env             = None,
                   cwd             = None,
                   shell           = False,
                   eval_includes   = None,
                   update_env      = False,
                   priority        = 5,
                   callback_func   = None,
                   callback_args   = (),
                   callback_kwargs = {},
                   fallback_func   = None,
                   fallback_args   = (),
                   fallback_kwargs = {}):
        """
        Run a command asynchronously in a new process monitored by the user thread pool.

        Python code or shell code can be used based on the shell switch. 
        If shell=False then all code defined in getConfig('DIRAC')['DiracCommandFiles']
        is prepended before the execution. This means that unless this config
        variable has been altered, the DIRAC API is available and a Dirac() object
        is already set up and called dirac. ( see help(diracAPI) )
        This makes it trivial to replace the diracAPI command using:
        
        In[0]: queues.addProcess('print dirac.status([123])')

        By default the command is executed within the DIRAC environment 
        meaning that one can use all the DIRAC command line tools easily as
        well

        In[1]: queues.addProcess('dirac-dms-user-lfns --help', shell=True)

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
        if type(command)!= str:
            logger.error("Input command must be of type 'string'")
            return

        from GangaDirac.BOOT import dirac_ganga_server
        if env is None: # rather than have getDiracEnv() as default in arg list as looks messy in help ;-)
            env = getDiracEnv()
        dirac_ganga_server.execute_nonblocking( command,
                                                timeout          = timeout,
                                                env              = env,
                                                cwd              = cwd,
                                                shell            = shell,
                                                eval_includes    = eval_includes,
                                                update_env       = update_env,
                                                priority         = priority,
                                                callback_func    = callback_func,
                                                callback_args    = callback_args,
                                                callback_kwargs  = callback_kwargs,
                                                fallback_func    = fallback_func,
                                                fallback_args    = fallback_args,
                                                fallback_kwargs  = fallback_kwargs )
