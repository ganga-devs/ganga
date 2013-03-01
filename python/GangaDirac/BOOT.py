
from GangaDirac.Lib.Utilities.DiracUtilities import getDiracEnv
from Ganga.Runtime.GPIexport                 import exportToGPI
from Ganga.GPIDev.Base.Proxy                 import addProxy, stripProxy
from Ganga.Utility.Config                    import getConfig
from Ganga.Utility.logging                   import getLogger
from Ganga.Utility.ColourText                import getColour
logger = getLogger()
    
def diracAPI(cmd, timeout = 60):
    '''Execute DIRAC API commands from w/in Ganga.

    The stdout will be returned, e.g.:
    
    # this will simply return 87
    diracAPI(\'print 87\')
    
    # this will return the status of job 66
    # note a Dirac() object is already provided set up as \'dirac\'
    diracAPI(\'print Dirac().status([66])\')
    diracAPI(\'print dirac.status([66])\')

    # or can achieve the same using command defined and included from
    # getConfig('DIRAC')['DiracCommandFiles']
    diracAPI(\'status([66])\')

    '''
    #    from GangaDirac.Lib.Backends.Dirac import Dirac
    from Ganga.GPI import Dirac
    return Dirac._impl.execAPI(cmd, timeout)
    #from GangaDirac.Lib.Backends.DiracBase import DiracBase
    #return DiracBase.execAPI(cmd, priority, timeout)

exportToGPI('diracAPI',diracAPI,'Functions')

def diracAPI_async(cmd, timeout = 120):
    '''Execute DIRAC API commands from w/in Ganga.
    '''
    from GangaDirac.Lib.Backends.DiracBase import dirac_ganga_server
    return dirac_ganga_server.execute_nonblocking(cmd, timeout, priority = 2)

exportToGPI('diracAPI_async',diracAPI_async,'Functions')

#def queue(worker_code, args=(), kwargs={}, priority = 4):
#    from GangaDirac.Lib.Backends.DiracBase import dirac_ganga_server
#    return dirac_ganga_server.execute_nonblocking(None,
#                                                  timeout       = None,
#                                                  cwd           = None,
#                                                  env           = None,
#                                                  shell         = True,
#                                                  priority      = priority,
#                                                  callback_func = worker_code,
#                                                  args          = args,
#                                                  kwds          = kwargs)
#exportToGPI('queue',queue,'Functions')

class queues(object):
    '''
    This class displays the user and monitor thread pools and associated queues

    The number of worker threads in the pool is controlled by
    the getConfig('DIRAC')['NumWorkerThreads'] config option.
    '''
    def _display(self, i):
        '''Return the current status of the thread pools and queues.'''
        from GangaDirac.Lib.Backends.DiracBase import dirac_ganga_server, dirac_monitoring_server
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
            output+= '{0:<21} {1:<33} {2:<10} | {3:<21} {4:<33} {5:<10}\n'.format(name_user, u[1][:30], u[2], name_monitor, m[1][:30], m[2])

        output+= '\n'
        output+= "Ganga user queue:\n"
        output+= "----------------\n"
        output+= str([i.command_input.command for i in dirac_ganga_server.get_queue()])
        
        output+= '\n'
        output+= "Ganga monitoring queue:\n"
        output+= "----------------------\n"
        output+= str([i.command_input.command for i in dirac_monitoring_server.get_queue()])
        return output
        #from Ganga.GPI import Dirac
        #return Dirac._impl.getQueues()

    def add(self, worker_code, args=(), kwargs={}, priority = 5):
        """
        Run any python callable object asynchronously through the user thread pool
        
        Code added to the queue will remain there until a free worker becomes
        available and picks it from the list. At this point the code will run
        asynchronously leaving the interpreter prompt unblocked and ready for user
        input.

        A simple example might be to download user data which would in the
        past block the prompt. Here I use job #72 and '/home/alex' arbitrarily
        for more info about the getOutputData command type help(getOutputData).

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
        from GangaDirac.Lib.Backends.DiracBase import dirac_ganga_server
        return dirac_ganga_server.execute_nonblocking(None,
                                                      callback_func = worker_code,
                                                      args          = args,
                                                      kwds          = kwargs,
                                                      priority      = priority)
#         return queue(worker_code, args, kwargs, priority)

    def addProcess(self, 
                   command, 
                   timeout       = getConfig('DIRAC')['Timeout'],
                   env           = None,
                   cwd           = None,
                   shell         = False,
                   priority      = 5,
                   callback_func = None,
                   args          = (),
                   kwds          = {}):
        """
        Run a command asynchronously in a new process monitored by the user thread pool.

        Python code or shell code can be used based on the shell switch. 
        If shell=False then all code defined in getConfig('DIRAC')['DiracCommandFiles']
        is prepended before the execution. This means that unless this config
        variable has been altered, the DIRAC API is available and a Dirac() object
        is already set up and called dirac. ( see help(diracAPI) )
        This makes it trivial to replace the diracAPI command using:
        
        In[0]: queues.addProcess('print dirac.status[123]')

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
                   command       = The command to run as a string 
                   timeout       = timeout for the command as int or None
                   env           = environment to run command in as dict or None
                   cwd           = working dir to run command in as string
                   shell         = True/False whether to interpret the
                                   command as a python or shell command
                   priority      = The thread queuing system is a priority
                                   queue with lower number = higher priority.
                                   This then should be an int normally 0-9
                   callback_func = Any python callable object. This is called
                                   once the command has finished running (or 
                                   timed out) and must take at least one arg.
                                   This first arg will be the stdout of the 
                                   executed command. This arg will be the
                                   result of eval(stdout) such that '{}' will
                                   become a dict. Fall back to str representation
                                   if the eval fails.
                             
                   args          = Any additional args to the callback_func 
                                   are specified here as a tuple
                   kwds          = kwargs for the callback_func are given here
                                   as a dict.
        """
        from GangaDirac.Lib.Backends.DiracBase import dirac_ganga_server
        if env is None: # rather than have getDiracEnv() as default in arg list as looks messy in help ;-)
            env = getDiracEnv()
        return dirac_ganga_server.execute_nonblocking(command, 
                                                      timeout,
                                                      env,
                                                      cwd,
                                                      shell,
                                                      priority,
                                                      callback_func,
                                                      args,
                                                      kwds)
exportToGPI('queues',queues(),'Classes')

#def killDiracServer():
#    '''Kills the child proces which runs the DIRAC server.'''
#    #from GangaDirac.Lib.Backends.Dirac import Dirac
#    #from GangaDirac.Lib.Backends.DiracBase import DiracBase
#    from Ganga.GPI import Dirac
#    return Dirac._impl.killServer()

#exportToGPI('killDiracServer',killDiracServer,'Functions')    


def getDiracFiles():
    import os
    from Ganga.GPI import DiracFile
    from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
    filename = getConfig('DIRAC')['DiracLFNBase'].replace('/','-') + '.lfns'
    logger.info('Creating list, please wait...')
    os.system('dirac-dms-user-lfns &> /dev/null')
    g=GangaList()
    with open(filename[1:],'r') as lfnlist:
        lfnlist.seek(0)
        g.extend((DiracFile(lfn='%s'% lfn.strip()) for lfn in lfnlist.readlines()))
    return addProxy(g)
 
exportToGPI('getDiracFiles',getDiracFiles,'Functions')


def dumpObject(object, filename):
    import pickle
    f=open(filename, 'wb')
    pickle.dump(stripProxy(object), f)
    f.close()
exportToGPI('dumpObject',dumpObject,'Functions')

def loadObject(filename):
    import pickle
    f=open(filename, 'rb')
    r = pickle.load(f)
    f.close()
    return addProxy(r)
exportToGPI('loadObject',loadObject,'Functions')
