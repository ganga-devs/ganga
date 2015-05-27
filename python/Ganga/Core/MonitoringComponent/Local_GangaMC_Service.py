import Queue, threading, time

from Ganga.Core.GangaThread import GangaThread
from Ganga.Core.GangaRepository import RegistryKeyError, RegistryLockError

import sys
if sys.hexversion >= 0x020600F0:
    Set = set
else:
    from sets import Set

from Ganga.Utility.threads import SynchronisedObject
from Ganga.Utility.util import IList

import Ganga.GPIDev.Credentials as Credentials
from Ganga.Core.InternalServices import Coordinator 

# Setup logging ---------------
import Ganga.Utility.logging
log = Ganga.Utility.logging.getLogger()

from Ganga.Core import BackendError
import Ganga.GPIDev.Adapters.IBackend
import Ganga.Utility.Config
config = Ganga.Utility.Config.makeConfig( 'PollThread', 'background job status monitoring and output retrieval' )

from Ganga.Core import GangaException

# some defaults
config.addOption( 'repeat_messages',False,'if 0 then log only once the errors for a given backend and do not repeat them anymore')
config.addOption( 'autostart',True,'enable monitoring automatically at startup, in script mode monitoring is disabled by default, in interactive mode it is enabled', type=type(True)) # enable monitoring on startup
config.addOption( 'base_poll_rate', 2,'internal supervising thread',hidden=1)
config.addOption( 'MaxNumResubmits', 5,'Maximum number of automatic job resubmits to do before giving')
config.addOption( 'MaxFracForResubmit', 0.25,'Maximum fraction of failed jobs before stopping automatic resubmission')
config.addOption( 'update_thread_pool_size' , 5,'Size of the thread pool. Each threads monitors a specific backaend at a given time. Minimum value is one, preferably set to the number_of_backends + 1')
config.addOption( 'default_backend_poll_rate' , 30,'Default rate for polling job status in the thread pool. This is the default value for all backends.')
config.addOption( 'Local' , 10,'Poll rate for Local backend.')
config.addOption( 'LCG' , 30,'Poll rate for LCG backend.')
config.addOption( 'Condor' , 30,'Poll rate for Condor backend.')
config.addOption( 'gLite' , 30,'Poll rate for gLite backend.')
config.addOption( 'LSF' , 20,'Poll rate for LSF backend.')
config.addOption( 'PBS' , 20,'Poll rate for PBS backend.')
config.addOption( 'Dirac' , 50,'Poll rate for Dirac backend.')
config.addOption( 'Panda' , 50,'Poll rate for Panda backend.')
#config.addOption( 'TestSubmitter', 1, 'Poll rate for TestSubmitter')

#Note: the rate of this callback is actually  MAX(base_poll_rate,callbacks_poll_rate)
config.addOption( 'creds_poll_rate', 30, "The frequency in seconds for credentials checker")
config.addOption( 'diskspace_poll_rate', 30, "The frequency in seconds for free disk checker")
config.addOption( 'DiskSpaceChecker', "", "disk space checking callback. This function should return False when there is no disk space available, True otherwise")

config.addOption( 'MaxNumberParallelMonitor', 50, "Maximum number of (sub)jobs to be passed to the backend for monitoring at once" )

#config.addOption( 'max_shutdown_retries',5,'OBSOLETE: this option has no effect anymore')


THREAD_POOL_SIZE = config[ 'update_thread_pool_size' ]
Qin = Queue.Queue()
ThreadPool = []
#number of threads waiting for actions in Qin
tpFreeThreads = 0

# The JobAction class encapsulates a function, its arguments and its post result action 
# based on what is defined as a successful run of the function. 
class JobAction( object ):
   def __init__( self, function, args = (), kwargs = {}, 
                 success = ( True, ),
                 callback_Success = lambda:None,
                 callback_Failure = lambda:None ):
      self.function = function
      self.args = args
      
      self.kwargs = kwargs
      self.success = success
      self.callback_Success = callback_Success
      self.callback_Failure = callback_Failure
      self.thread = None
      self.description = ''

class MonitoringWorkerThread(GangaThread):
   def __init__(self,name):
      GangaThread.__init__(self, name)

   def run(self):
      self._execUpdateAction()

   # This function takes a JobAction object from the Qin queue, 
   # executes the embedded function and runs post result actions.
   def _execUpdateAction(self):
      global tpFreeThreads
      ##DEBUGGING THREADS
      ##import sys
      ##sys.settrace(_trace)
      while not self.should_stop():
         log.debug( "%s waiting..." % threading.currentThread() )
         #setattr(threading.currentThread(), 'action', None)
         tpFreeThreads+=1

         from Queue import Empty
         while not self.should_stop():
             try:
                 action = Qin.get(block=True,timeout=0.5)
                 break
             except Queue.Empty:
                 continue
             except Empty:
                 continue

         if self.should_stop():
             break

         tpFreeThreads-=1      
         #setattr(threading.currentThread(), 'action', action)  
         log.debug( "Qin's size is currently: %d" % Qin.qsize() )
         log.debug( "%s running..." % threading.currentThread() )

         if not isinstance( action, JobAction ):
            continue
         if action.function == 'stop':
            break
         try:
            result = action.function( *action.args, **action.kwargs )
         except:
            action.callback_Failure()
         else:
            if result in action.success:
               action.callback_Success()
            else:
               action.callback_Failure()

# Create the thread pool
def _makeThreadPool( threadPoolSize = THREAD_POOL_SIZE, daemonic = True ):
   for i in range( THREAD_POOL_SIZE ):
      t = MonitoringWorkerThread( name = "MonitoringWorker_%s" % i)
      ThreadPool.append( t )
      t.start()

def stop_and_free_thread_pool(fail_cb=None, max_retries=5):
   """
    Clean shutdown of the thread pool. 
    A failed attempt to stop all the worker threads is followed by a call to the supplied callback which
    decides if a new attempt is performed or not.
    Example for a decision callback:      
         def myfail_cb():
            resp = raw_input("The cleanup procedures did not complete yet. Do you want to wait more?[y/N]")
            return resp.lower()=='y'
   """
   
   def join_worker_threads(threads,timeout=3):
      for t in threads:
         if t.isAlive():
            t.join(timeout)
   
   for i in range( len( ThreadPool ) ):
      Qin.put( JobAction( 'stop' ) )

   join_worker_threads(ThreadPool)
   
   #clean shutdown of stalled threads
   while True:
      if not fail_cb or max_retries<=0:
         break
      stalled = [t for t in ThreadPool if t.isAlive()]
      if not stalled:
         break 
      if fail_cb(): #continue?
         join_worker_threads(stalled,timeout=3)
         max_retries-=1
      else:
         break
                
   del ThreadPool[:]

# purge Qin
def _purge_actions_queue():
   """
   Purge Qin: consume the current queued actions 
   Note: the producer (i.e JobRegistry_Monitor) should be stopped before the method is called
   """
   from Queue import Empty
   #purge the queue
   for i in range(len(Qin.queue)):
       try:
           action=Qin.get_nowait()
           #let the *stop* action in queue, otherwise the worker threads will fail to terminate
           if isinstance( action, JobAction ) and action.function == 'stop':
               Qin.put( action )
       except Empty:
           break

if config['autostart']:
    _makeThreadPool()


# Each entry for the updateDict_ts object (based on the UpdateDict class) is a _DictEntry object.
class _DictEntry( object ):
   def __init__( self, backendObj, jobSet, entryLock, timeoutCounterMax ):
      self.backendObj = backendObj
      self.jobSet = jobSet
      self.entryLock = entryLock
      self.timeoutCounterMax = timeoutCounterMax
      self.timeoutCounter = timeoutCounterMax - 0.01
      self.timeLastUpdate = 0.0
   
   def updateActionTuple( self ):
      return self.backendObj, self.jobSet, self.entryLock


class UpdateDict( object ):
   """
   This serves as the Update Table. Is is meant to be used 
   by wrapping it as a SynchronisedObject so as to ensure thread safety.
   """
   def __init__( self ):
      self.table = {}
   
   def addEntry( self, backendObj, backendCheckingFunction, jobList, timeoutMax = None ):
      if not jobList:
         return
      if timeoutMax is None:
         timeoutMax = config[ 'default_backend_poll_rate' ]
      log.debug( "\n*----addEntry()" )
      backend = backendObj._name
      try:
         backendObj, jSet, lock = self.table[ backend ].updateActionTuple()
      except KeyError: # New backend.
         self.table[ backend ] = _DictEntry( backendObj, Set( jobList ), threading.RLock(), timeoutMax )
         Qin.put( JobAction( backendCheckingFunction, self.table[ backend ].updateActionTuple() ) ) # queue to get processed
         log.debug( "**Adding %s to new %s backend entry." % ( [x.id for x in jobList], backend ) )
         return True
      else:
         # backend is in Qin waiting to be processed. Increase it's list of jobs
         # by updating the table entry accordingly. This will reduce the 
         # number of update requests.
         # i.e. It's like getting a friend in the queue to pay for your purchases as well! ;p
         log.debug( "*: backend=%s, isLocked=%s, isOwner=%s, joblist=%s, queue=%s" % (backend, lock._RLock__count, lock._is_owned(), [x.id for x in jobList], Qin.qsize() ) )
         if lock.acquire( False ):
            try:
               jSetSize = len( jSet )
               log.debug( "Lock acquire successful. Updating jSet %s with %s." % ( [x.id for x in jSet], [x.id for x in jobList] ) )
               jSet.update( jobList )
               # If jSet is empty it was cleared by an update action 
               # i.e. the queue does not contain an update action for the particular backend any more.
               if jSetSize: # jSet not cleared
                   log.debug( "%s backend job set exists. Added %s to it." % ( backend, [x.id for x in jobList] ) )
               else:
                   Qin.put( JobAction( backendCheckingFunction, self.table[ backend ].updateActionTuple() ) )
                   log.debug( "Added new %s backend update action for jobs %s." % ( backend, [ x.id for x in self.table[ backend ].updateActionTuple()[1] ] ) )
            finally:
               lock.release()
            log.debug( "**: backend=%s, isLocked=%s, isOwner=%s, joblist=%s, queue=%s" % (backend, lock._RLock__count, lock._is_owned(), [x.id for x in jobList], Qin.qsize() ) )
            return True
         else:
            log.debug( "Could not acquire lock for %s backend. addEntry() skipped." % backend )
            log.debug( "**: backend=%s, isLocked=%s, isOwner=%s, joblist=%s, queue=%s" % (backend, lock._RLock__count, lock._is_owned(), [x.id for x in jobList], Qin.qsize() ) )
            return False

   def clearEntry( self, backend ):
      try:
         entry = self.table[ backend ]
      except KeyError:
         log.error( "Error clearing the %s backend. It does not exist!" % backend )
      else:
         entry.jobSet = Set()
         entry.timeoutCounter = entry.timeoutCounterMax

   def timeoutCheck( self ):
      for backend, entry in self.table.items():
         # timeoutCounter is reset to its max value ONLY by a successful update action.
         #
         # Initial value and subsequent resets by timeoutCheck() will set the timeoutCounter
         # to a value just short of the max value to ensure that it the timeoutCounter is
         # not decremented simply because there are no updates occuring.
         if not entry.entryLock._RLock__count and \
            entry.timeoutCounter == entry.timeoutCounterMax and \
            entry.entryLock.acquire( False ):
            log.debug( "%s has been reset. Acquired lock to begin countdown." % backend )
            entry.timeLastUpdate = time.time()
         # decrease timeout counter
         if entry.entryLock._is_owned():
            if entry.timeoutCounter <= 0.0:
               entry.timeoutCounter = entry.timeoutCounterMax - 0.01
               entry.timeLastUpdate = time.time()
               entry.entryLock.release()
               log.debug( "%s backend counter timeout. Resetting to %s." % ( backend, entry.timeoutCounter ) )
            else:
               _l = time.time()
               entry.timeoutCounter -= _l - entry.timeLastUpdate
               entry.timeLastUpdate = _l
#               log.debug( "%s backend counter is %s." % ( backend, entry.timeoutCounter ) )

   def isBackendLocked( self, backend ):
      try:
         return bool( self.table[ backend ].entryLock._RLock__count )
      except KeyError:
         return False
 
   def releaseLocks( self ):
       for backend, entry in self.table.items():
           if entry.entryLock._is_owned():
               entry.entryLock.release()

updateDict_ts = SynchronisedObject( UpdateDict() )


class CallbackHookEntry( object ):
    def __init__( self, argDict, enabled = True, timeout = 0):
        self.argDict = argDict
        self.enabled = enabled
        #the frequency in seconds
        self.timeout = timeout
        #record the time when this hook has been run        
        self._lastRun = 0

class JobRegistry_Monitor( GangaThread ):
    """Job monitoring service thread."""
    uPollRate = 0.5
    minPollRate = 1.0
    def __init__(self, registry ):
        GangaThread.__init__( self, name = "JobRegistry_Monitor" )
        self.setDaemon( True )
        self.registry = registry
        self.__sleepCounter = 0.0 
        self.__updateTimeStamp = time.time()
        self.progressCallback = lambda x:None
        self.callbackHookDict = {}
        self.clientCallbackDict = {}
        self.alive = True
        self.enabled = False
        #run the monitoring loop continuosly (steps=-1) or just a specified number of steps(>0)
        self.steps = -1
        self.activeBackends = {}
        self.updateJobStatus = None
        self.defaultUpdateJobStatus = None
        self.errors = {}
        # Create the default backend update method and add to callback hook.
        self.makeUpdateJobStatusFunction()
        
        # Add credential checking to monitoring loop
        for _credObj in Credentials._allCredentials.itervalues():
            log.debug( "Setting callback hook for %s" % _credObj._name )
            self.setCallbackHook( self.makeCredCheckJobInsertor( _credObj ), {}, True, timeout=config[ 'creds_poll_rate'] )
            
        # Add low disk-space checking to monitoring loop
        log.debug( "Setting callback hook for disk space checking")
        self.setCallbackHook( self.diskSpaceCheckJobInsertor, {}, True, timeout=config[ 'diskspace_poll_rate'] )
        
        #synch objects
        #main loop mutex
        self.__mainLoopCond=threading.Condition()
        #cleanup synch
        self.__cleanUpEvent=threading.Event() 
        #asynch mon loop running synch 
        self.__monStepsTerminatedEvent=threading.Event()
        #event to signal the break of job lists iterators
        self.stopIter=threading.Event()
        self.stopIter.set()
        
    def run( self ):
        """
        Main monitoring loop
        """
        import thread
        Ganga.Core.MonitoringComponent.monitoring_thread_id = thread.get_ident()
        del thread
        
        while self.alive:
            #synchronize the main loop since we can get disable requests 
            self.__mainLoopCond.acquire()
            try:
                log.debug( 'Monitoring loop lock acquired. Running loop' )                
                #we are blocked here while the loop is disabled
                while not self.enabled:
                    self.__cleanUp()
                    if not self.alive: #stopped?
                       return
                    #disabled,but still alive, so we keep waiting 
                    #for i in range( int(self.uPollRate*20) ):
                    #    if not self.alive:
                    #        return
                    #    self.__mainLoopCond.wait( self.uPollRate*0.05 )
                    self.__mainLoopCond.wait()

                self.__monStep()

                #delay here the monitoring steps according to the configuration                
                while self.__sleepCounter > 0.0:
                    self.progressCallback( self.__sleepCounter )
                    for i in range( int(self.uPollRate*20) ):
                        if self.enabled:
                            self.__mainLoopCond.wait( self.uPollRate*0.05 )
                    if not self.enabled:
                        if not self.alive: #stopped?
                            self.__cleanUp()
                        #disabled, break to the outer while
                        break        
                    else:
                       self.__sleepCounter -= self.uPollRate
                       
                else:        
                    #run on demand?
                    if self.steps>0:
                        #decrement the remaining number of steps to run
                        self.steps-=1
                        #requested number of steps executed, disabling...
                        if self.steps<=0:
                            self.enabled=False
                            #notify the blocking call of runMonitoring()
                            self.__monStepsTerminatedEvent.set()  
            finally:
                self.__mainLoopCond.release()
                
        #final cleanup
        self.__cleanUp()
         
    def __monStep(self):
        """
        A single monitoring step in the monitoring loop
        Note:
        Internally the step does not block, it produces *actions* that are queued to be run 
        in the thread pool
        """
        if not self.callbackHookDict:
            log.error( 'No callback hooks registered' )
            return  
        for cbHookFunc in self.callbackHookDict.keys():
            try:
                cbHookEntry = self.callbackHookDict[ cbHookFunc ]
            except KeyError:
                continue
            if cbHookEntry.enabled and time.time()-cbHookEntry._lastRun>=cbHookEntry.timeout:                
                log.debug("Running monitoring callback hook function %s(**%s)" % (cbHookFunc, cbHookEntry.argDict))
                cbHookFunc(**cbHookEntry.argDict)
                cbHookEntry._lastRun = time.time()
                
        self.runClientCallbacks()
        self.__updateTimeStamp = time.time()
        self.__sleepCounter = config[ 'base_poll_rate' ]
        
    
    def runMonitoring( self, jobs=None, steps=1, timeout=60):
        """
        Enable/Run the monitoring loop and wait for the monitoring steps completion.
        Parameters:
          steps:   number of monitoring steps to run
          timeout: how long to wait for monitor steps termination (seconds)
          jobs: a registry slice to be monitored (None -> all jobs), it may be passed by the user so ._impl is stripped if needed
        Return:
          False, if the loop cannot be started or the timeout occured while waiting for monitoring termination
          True, if the monitoring steps were successfully executed  
        Note:         
          This method is meant to be used in Ganga scripts to request monitoring on demand. 
        """
    
        if not type(steps) is int or steps < 1:
            log.warning("The number of monitor steps should be a positive integer")
            return False
         
        if not self.alive:
            log.error("Cannot run the monitoring loop. It has already been stopped")
            return False

        #we don not allow the user's request the monitoring loop while the internal services are stopped                 
        if not Coordinator.servicesEnabled:
            log.error("Cannot run the monitoring loop." 
                      "The internal services are disabled (check your credentials or available disk space)")
            return False
         
        # if the monitoring is disabled (e.g. scripts) 
        if not self.enabled: 
           # and there are some required cred which are missing 
           # (the monitoring loop does not monitor the credentials so we need to check 'by hand' here)
           _missingCreds = Coordinator.getMissingCredentials()
           if _missingCreds:
              log.error("Cannot run the monitoring loop. The following credentials are required: %s" % _missingCreds)
              return False              

        self.__mainLoopCond.acquire()
        log.debug( 'Monitoring loop lock acquired. Enabling mon loop' )
        try:
            if self.enabled or self.__isInProgress():
                log.error("The monitoring loop is already running.")
                return False

            if jobs:
               try:
                  m_jobs = jobs._impl
               except AttributeError:
                  m_jobs = jobs

               # additional check if m_jobs is really a registry slice
               # the underlying code is not prepared to handle correctly the situation if it is not
               from Ganga.GPIDev.Lib.Registry.RegistrySlice import RegistrySlice
               if not isinstance(m_jobs, RegistrySlice):
                   log.warning('runMonitoring: jobs argument must be a registry slice such as a result of jobs.select() or jobs[i1:i2]')
                   return False

               self.registry = m_jobs

            #enable mon loop
            self.enabled = True
            #set how many steps to run
            self.steps = steps
            #enable job list iterators
            self.stopIter.clear()
            # Start backend update timeout checking.
            self.setCallbackHook( updateDict_ts.timeoutCheck, {}, True )
            
            #wake up the mon loop
            self.__mainLoopCond.notifyAll()
        finally:
            self.__mainLoopCond.release()
            
        #wait to execute the steps
        self.__monStepsTerminatedEvent.wait()
        self.__monStepsTerminatedEvent.clear()
        #wait the steps to be executed or timeout to occur      
        if not self.__awaitTermination(timeout):
            log.warning("Monitoring loop started but did not complete in the given timeout.")
            #force loops termination
            self.stopIter.set()
            return False
        return True

    def enableMonitoring( self ):
        """
        Run the monitoring loop continuously
        """
        
        if not self.alive:
            log.error("Cannot start monitoring loop. It has already been stopped")
            return False
        
        self.__mainLoopCond.acquire() 
        log.debug( 'Monitoring loop lock acquired. Enabling mon loop' )
        try: 
            self.enabled = True
            #infinite loops
            self.steps=-1
            #enable job list iterators
            self.stopIter.clear()
            log.debug( 'Monitoring loop enabled' )
            # Start backend update timeout checking.
            self.setCallbackHook( updateDict_ts.timeoutCheck, {}, True )
            self.__mainLoopCond.notifyAll()              
        finally:
            self.__mainLoopCond.release()
        return True
            
    def disableMonitoring( self ):
        """
        Disable the monitoring loop
        """
        
        if not self.alive:
            log.error("Cannot disable monitoring loop. It has already been stopped")
            return False
        
        self.__mainLoopCond.acquire() 
        log.debug( 'Monitoring loop lock acquired. Disabling mon loop' )
        try:  
            self.enabled = False
            self.steps=-1
            self.stopIter.set()
            log.debug( 'Monitoring loop disabled' )
            #wake up the monitoring loop            
            self.__mainLoopCond.notifyAll()                
        finally:
            self.__mainLoopCond.release()
        #wait for cleanup        
        #self.__cleanUp()
        #self.__cleanUpEvent.wait()
        #self.__cleanUpEvent.clear()        
        return True
            
    def stop( self, fail_cb=None, max_retries=5 ):
        """
        Stop the monitoring loop
        Parameters:
         fail_cb : if not None, this callback is called if a retry attempt is needed
        """        
        if not self.alive:
            log.warning("Monitoring loop has already been stopped")
            return False
        
        self.__mainLoopCond.acquire()
        if self.enabled: log.info( 'Stopping the monitoring component...' )
        try: 
            #signal the main thread to finish
            self.alive = False
            self.enabled = False
            self.steps=-1        
            self.stopIter.set()
            #wake up the monitoring loop            
            self.__mainLoopCond.notifyAll()
        finally:
            self.__mainLoopCond.release()
        #wait for cleanup        
        self.__cleanUpEvent.wait()
        self.__cleanUpEvent.clear()

        ### ---->
        ###wait for all worker threads to finish
        ###self.__awaitTermination()
        ###join the worker threads
        ###stop_and_free_thread_pool(fail_cb,max_retries)
        ###log.info( 'Monitoring component stopped successfully!' )
        return True
    
    def __cleanUp(self):
        """
        Cleanup function ran in JobRegistry_Monitor thread to disable the monitoring loop
        updateDict_ts.timeoutCheck can hold timeout locks that need to be released 
        in order to allow the pool threads to be freed.
        """

        #cleanup the global Qin
        _purge_actions_queue()
        #release timeout check locks
        timeoutCheck = updateDict_ts.timeoutCheck
        if self.callbackHookDict.has_key( timeoutCheck ):
            updateDict_ts.releaseLocks()
            self.removeCallbackHook( timeoutCheck )
        #wake up the calls waiting for cleanup
        self.__cleanUpEvent.set()
        
    def __isInProgress(self):
       return self.steps>0 or Qin.qsize()>0 or tpFreeThreads<len( ThreadPool )
     
    def __awaitTermination( self,timeout=5 ):
       """
        Wait for resources to be cleaned up (threads,queue)
        Returns:
            False, on timeout
       """    
       while self.__isInProgress():                      
                time.sleep(self.uPollRate)
                timeout-=self.uPollRate                
                if timeout<=0:
                    return False
       return True
            
    
    def setCallbackHook( self, func, argDict, enabled, timeout=0 ):
        log.debug( 'Setting Callback hook function %s.' % func )
        if func in self.callbackHookDict:
            log.debug( 'Replacing existing callback hook function with %s' % func )
        self.callbackHookDict[ func ] = CallbackHookEntry( argDict = argDict, enabled = enabled, timeout=timeout )

    def removeCallbackHook( self, func ):
        log.debug( 'Removing Callback hook function %s.' % func )
        try:
            del self.callbackHookDict[ func ]
        except KeyError:
            log.error( 'Callback hook function does not exist.' )
    
    def enableCallbackHook( self, func ):
        log.debug( 'Enabling Callback hook function %s.' % func )
        try:
            self.callbackHookDict[ func ].enabled = True
        except KeyError:
            log.error( 'Callback hook function does not exist.' )
    
    def disableCallbackHook( self, func ):
        log.debug( 'Disabling Callback hook function %s.' % func )
        try:
            self.callbackHookDict[ func ].enabled = False
        except KeyError:
            log.error( 'Callback hook function does not exist.' )

    def runClientCallbacks( self ):
        for clientFunc in self.clientCallbackDict:
            log.debug( 'Running client callback hook function %s(**%s).' % ( clientFunc, self.clientCallbackDict[ clientFunc ] ) )
            clientFunc( **self.clientCallbackDict[ clientFunc ] )

    def setClientCallback( self, clientFunc, argDict ):
        log.debug( 'Setting client callback hook function %s(**%s).' % ( clientFunc, argDict ) )
        try:
            self.clientCallbackDict[ clientFunc ] = argDict
        except KeyError:
            log.error( "Callback hook function not found." )
    
    def removeClientCallback( self, clientFunc ):
        log.debug( 'Removing client callback hook function %s.' % clientFunc )
        try:
            del self.clientCallbackDict[ clientFunc ]
        except KeyError:
            log.error( "%s not found in client callback dictionary." % clientFunc.__name__ )

    def __defaultActiveBackendsFunc( self ):
        active_backends = {}
        # FIXME: this is not thread safe: if the new jobs are added then iteration exception is raised
        fixed_ids = self.registry.ids()
        for i in fixed_ids:
            try:
                j = self.registry(i)
                if j.status in [ 'submitted', 'running' ]:
                    #j._getWriteAccess()
                    bn = j.backend._name
                    active_backends.setdefault( bn, [] )
                    active_backends[ bn ].append( j )
                elif j.status in [ 'submitting' ]:
                    if j.count_subjobs() > 0:
                        #j._getWriteAccess()
                        bn = j.backend._name
                        active_backends.setdefault( bn, [] )
                        active_backends[ bn ].append( j )
            except RegistryKeyError, x:
                pass # the job was removed
            except RegistryLockError, x:
                pass # the job was removed
        return active_backends

    def makeUpdateJobStatusFunction( self, makeActiveBackendsFunc = None ):

        def _returnMonitorableJobs( jobList ):

            #log.info( "%s" % str( jobList ) )

            returnableSet = Set([])#IList()

            for job in jobList:
                if job.status in [ 'submitted', 'running' ]:

                    returnableSet.add( job )

                    size = 0
                    for i in returnableSet:
                        if i.count_subjobs() > 0:
                            size = size + i.count_subjobs()
                        else:
                            size = size + 1

                    if size > config['MaxNumberParallelMonitor']:
                        break

            return returnableSet, size
            ## Function to replace this simpler IList description
            #IList(filter( lambda x: x.status in [ 'submitted', 'running' ], jobListSet ), self.stopIter)

        def _returnMonitorableSubJobs( jobList, found ):

            returnableSet = Set([])#IList()

            for job in jobList:

                if job.status in [ 'submitting' ] and job.count_subjobs() > 0:

                    returnableSet.add( job )

                    size = 0
                    for i in returnableSet:
                        size = size + job.count_subjobs()

                    if size > (config['MaxNumberParallelMonitor'] - found):
                        break

            return returnableSet

            ## Function to replace the simpler IList description
            #IList(filter( lambda x: ( (x.status in [ 'submitting' ]) and (len(x.subjobs) > 0) ), jobListSet ), self.stopIter)

        def checkBackend( backendObj, jobListSet, lock ): # This function will be run by update threads
            currentThread = threading.currentThread()
            lock.acquire() # timeout mechanism may have acquired the lock to impose delay.
            try:
                log.debug( "[Update Thread %s] Lock acquired for %s" % ( currentThread, backendObj._name ) )


                ##  I expect this to be an expensive function as it can trigger loading from disk several large jobs from disk
                ##  Can we optimise this a little perhaps as this can lead to a slowdown in monitoring startup
                ##  Can python islice fix this? rcurrie

                ##  All standard jobs to be monitored
                alljobList_fromset, found = _returnMonitorableJobs( jobListSet )

                max_parallel = config['MaxNumberParallelMonitor'] - found

                if len(alljobList_fromset) < max_parallel:
                    ##  All jobs which may have subjobs to be monitored
                    masterJobList_fromset = _returnMonitorableSubJobs( jobListSet, found )
                else:
                    masterJobList_fromset = Set([])

                ## Combine both lists
                jobList_fromset = list( alljobList_fromset.union( masterJobList_fromset ) )

                #log.info( "%s" % str(jobList_fromset) )

                updateDict_ts.clearEntry( backendObj._name )
                try:
                    log.debug( "[Update Thread %s] Updating %s with %s." % ( currentThread, backendObj._name, [x.id for x in jobList_fromset ] ) )
                    for j in jobList_fromset:
                        if hasattr(j, 'backend'):
                            if hasattr(j.backend,'setup'):
                                j.backend.setup()


                    ##  It was observed we were saturating some methods in the backend by requesting to
                    ##  update several hundred subjobs when a job is submitted
                    ##  In order to fix this add configurable to only pass n many jobs to the backend
                    import math
                    if max_parallel > 0:
                        for batch_num in range(int( math.ceil( float(len(jobList_fromset)) * (1./float(max_parallel)) ) )):
                            backendObj.master_updateMonitoringInformation( jobList_fromset[ batch_num* max_parallel : (batch_num+1) * max_parallel ] )
                    else:
                        backendObj.master_updateMonitoringInformation( jobList_fromset )


                    # resubmit if required
                    for j in jobList_fromset:

                        if not j.do_auto_resubmit:
                            continue

                        if j.count_subjobs() == 0:
                            try_resubmit = j.info.submit_counter <= config['MaxNumResubmits']
                        else:
                            # Check for max number of resubmissions
                            skip = False
                            for s in j.subjobs:
                                if s.info.submit_counter > config['MaxNumResubmits'] or s.status == "killed":
                                    skip = True

                            if skip:
                                continue

                            num_com = len( [s for s in j.subjobs if s.status in ['completed'] ] )
                            num_fail = len( [s for s in j.subjobs if s.status in ['failed'] ] )

                            #log.critical('Checking failed subjobs for job %d... %d %s',j.id,num_com,num_fail)
                            
                            try_resubmit = num_fail > 0 and (float(num_fail) / float(num_com+num_fail)) < config['MaxFracForResubmit']
                        
                        if try_resubmit:
                            if j.backend.check_auto_resubmit():
                                log.warning('Auto-resubmit job %d...' % j.id)
                                j.auto_resubmit()
                            
                except BackendError, x:
                    self._handleError( x, x.backend_name, 0 )
                except Exception, x:
                    self._handleError( x, backendObj._name, 1 )
                log.debug( "[Update Thread %s] Flushing registry %s." % ( currentThread, [x.id for x in jobList_fromset ] ) )
                self.registry._flush( jobList_fromset ) # Optimisation required! 
            except Exception, x:
                log.debug( "Monitoring Exception: %s" % str(x) )
            finally:
                lock.release()
                log.debug( "[Update Thread %s] Lock released for %s." % ( currentThread, backendObj._name ) )

        def f( activeBackendsFunc ):
            activeBackends = activeBackendsFunc()
            for jList in activeBackends.values():
                backendObj = jList[0].backend
                try:
                   pRate = config[ backendObj._name ]
                except:
                   pRate = config[ 'default_backend_poll_rate' ]
                # TODO: To include an if statement before adding entry to
                #       updateDict. Entry is added only if credential requirements
                #       of the particular backend is satisfied.
                #       This requires backends to hold relevant information on its
                #       credential requirements.
                updateDict_ts.addEntry( backendObj, checkBackend, jList, pRate )

        if makeActiveBackendsFunc is None:
            makeActiveBackendsFunc = self.__defaultActiveBackendsFunc

        if makeActiveBackendsFunc == self.__defaultActiveBackendsFunc:
            self.defaultUpdateJobStatus = f
        if self.updateJobStatus is not None:
            self.removeCallbackHook( self.updateJobStatus )
        self.updateJobStatus = f
        self.setCallbackHook( f, { 'activeBackendsFunc' : makeActiveBackendsFunc }, True )
        return self.updateJobStatus

    def makeCredCheckJobInsertor( self, credObj ):
        def credCheckJobInsertor():
            def cb_Success():
                self.enableCallbackHook( credCheckJobInsertor )
             
            def cb_Failure():
                self.enableCallbackHook( credCheckJobInsertor )
                self._handleError( '%s checking failed!' % credObj._name, credObj._name, 1 )

            log.debug( 'Inserting %s checking function to Qin.' % credObj._name )
            _action = JobAction( function = self.makeCredChecker( credObj ),
                                 callback_Success = cb_Success,
                                 callback_Failure = cb_Failure )
            self.disableCallbackHook( credCheckJobInsertor )
            try:
                Qin.put( _action )
            except:
                cb_Failure()
        return credCheckJobInsertor

    def makeCredChecker( self, credObj ):
        def credChecker():
            log.debug( "Checking %s." % credObj._name )
            try:
                s = credObj.renew()
            except Exception, msg:
                return False
            else:
                return s
        return credChecker
     
    
    def diskSpaceCheckJobInsertor(self):
       """
       Inserts the disk space checking task in the monitoring task queue
       """
       def cb_Success():
           self.enableCallbackHook(self.diskSpaceCheckJobInsertor)
        
       def cb_Failure():
           self.disableCallbackHook(self.diskSpaceCheckJobInsertor)
           self._handleError('Available disk space checking failed and it has been disabled!', 'DiskSpaceChecker',False)

       log.debug('Inserting disk space checking function to Qin.')
       _action = JobAction(function = Coordinator._diskSpaceChecker,
                            callback_Success = cb_Success,
                            callback_Failure = cb_Failure)
       self.disableCallbackHook(self.diskSpaceCheckJobInsertor)
       try:
           Qin.put(_action)
       except:
           cb_Failure()

    def updateJobs( self ):
        if time.time() - self.__updateTimeStamp >= self.minPollRate:
            self.__sleepCounter = 0.0
        else:
            self.progressCallback( "Processing... Please wait." )
            log.debug( "Updates too close together... skipping latest update request." )
            self.__sleepCounter = self.minPollRate

    def _handleError( self, x, backend_name, show_traceback ):
        def log_error():
            log.error( 'Problem in the monitoring loop: %s', str( x ) )
            #import traceback
            #traceback.print_stack()
            if show_traceback:
                Ganga.Utility.logging.log_user_exception( log )
        bn = backend_name
        self.errors.setdefault( bn, 0 )
        if self.errors[ bn ] == 0:
            log_error()
            if not config[ 'repeat_messages' ]:
                log.info( 'Further error messages from %s handler in the monitoring loop will be skipped.' % bn )
        else:
            if config[ 'repeat_messages' ]:
                log_error()
        self.errors[ bn ] += 1      


######## THREAD POOL DEBUGGING ###########
def _trace(frame, event, arg):
    setattr(threading.currentThread(), '_frame', frame)
   
def getStackTrace():
    import inspect

    try:
        status = "Available threads:\n"

        for worker in ThreadPool:
            status = status + "  " + worker.getName() + ":\n"

            if hasattr( worker, '_frame' ):
                frame = worker._frame
                if frame:
                    status = status + "    stack:\n"
                    for frame, filename, line, function_name, context, index in inspect.getouterframes(frame):
                        status = status + "      " + function_name + " @ " + filename + " # " + str(line) + "\n"

            status = status + "\n"
        print "Queue",Qin.queue     
        return status
    finally:
        pass

