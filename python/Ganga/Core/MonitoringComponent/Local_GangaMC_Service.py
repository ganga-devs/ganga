import Queue
import threading
import time
import copy
from contextlib import contextmanager

from Ganga.Core.GangaThread import GangaThread
from Ganga.Core.GangaRepository import RegistryKeyError, RegistryLockError

from Ganga.Utility.threads import SynchronisedObject

import Ganga.GPIDev.Credentials as Credentials
from Ganga.Core.InternalServices import Coordinator

from Ganga.GPIDev.Base.Proxy import isType, stripProxy, getName, getRuntimeGPIObject

from Ganga.GPIDev.Lib.Job.Job import lazyLoadJobStatus, lazyLoadJobBackend

# Setup logging ---------------
from Ganga.Utility.logging import getLogger, log_unknown_exception, log_user_exception

from Ganga.Core import BackendError
from Ganga.Utility.Config import getConfig

from collections import defaultdict

log = getLogger()

config = getConfig("PollThread")
THREAD_POOL_SIZE = config['update_thread_pool_size']
Qin = Queue.Queue()
ThreadPool = []

heartbeat_times = None
global_start_time = None

# The JobAction class encapsulates a function, its arguments and its post result action
# based on what is defined as a successful run of the function.

class JobAction(object):

    def __init__(self, function, args=(), kwargs={},
                 success=(True, ),
                 callback_Success=lambda: None,
                 callback_Failure=lambda: None):
        self.function = function
        self.args = args

        self.kwargs = kwargs
        self.success = success
        self.callback_Success = callback_Success
        self.callback_Failure = callback_Failure
        self.thread = None
        self.description = ''

def getNumAliveThreads():
    num_currently_running_command = 0
    for this_thread in ThreadPool:
        if this_thread._currently_running_command:
            num_currently_running_command += 1
    return num_currently_running_command

def checkHeartBeat(global_count):

    latest_timeNow = time.time()

    for this_thread in ThreadPool:
        thread_name = this_thread._thread_name

        last_time = heartbeat_times[thread_name]

        dead_time = config['HeartBeatTimeOut']
        max_warnings = 5

        if (latest_timeNow - last_time) > dead_time and this_thread.isAlive()\
                and this_thread._currently_running_command is True\
                and global_count < max_warnings:

            log.warning("Thread: %s Has not updated the heartbeat in %ss!! It's possibly dead" %(thread_name, str(dead_time)))
            log.warning("Thread is attempting to execute: %s" % this_thread._running_cmd)
            log.warning("With arguments: (%s)" % str(this_thread._running_args))

            ## Add at least 5sec here to avoid spamming the user non-stop that a monitoring thread has locked up almost entirely
            ## You'll get a message at most once per 5sec or less if the Monitoring is busy/asleep
            heartbeat_times[thread_name] += 5.
            global_count+=1

class MonitoringWorkerThread(GangaThread):

    def __init__(self, name):
        GangaThread.__init__(self, name)
        self._currently_running_command = False
        self._running_cmd = None
        self._running_args = None
        self._thread_name = name

    def run(self):
        self._execUpdateAction()

    # This function takes a JobAction object from the Qin queue,
    # executes the embedded function and runs post result actions.
    def _execUpdateAction(self):
        # DEBUGGING THREADS
        # import sys
        # sys.settrace(_trace)
        while not self.should_stop():
            log.debug("%s waiting..." % threading.currentThread())
            #setattr(threading.currentThread(), 'action', None)

            heartbeat_times[self._thread_name] = time.time()

            while not self.should_stop():
                try:
                    action = Qin.get(block=True, timeout=0.5)
                    break
                except Queue.Empty:
                    continue

            if self.should_stop():
                break

            #setattr(threading.currentThread(), 'action', action)
            log.debug("Qin's size is currently: %d" % Qin.qsize())
            log.debug("%s running..." % threading.currentThread())
            self._currently_running_command = True
            if not isType(action, JobAction):
                continue
            if action.function == 'stop':
                break
            try:
                try:
                    self._running_cmd = action.function.__name__
                    self._running_args = []
                    for arg in action.args:
                        self._running_args.append("%s, " % arg)
                    for k, v in action.kwargs:
                        self._running_args.append("%s=%s, " % (str(k), str(v)))
                except:
                    self._running_cmd = "unknown"
                    self._running_args = []
                result = action.function(*action.args, **action.kwargs)
            except Exception as err:
                log.debug("_execUpdateAction: %s" % str(err))
                action.callback_Failure()
            else:
                if result in action.success:
                    action.callback_Success()
                else:
                    action.callback_Failure()

            self._running_args = None
            self._running_cmd = None
            self._currently_running_command = False

# Create the thread pool

def _makeThreadPool(threadPoolSize=THREAD_POOL_SIZE, daemonic=True):
    global ThreadPool, global_start_time, heartbeat_times
    global_start_time = time.time()
    if ThreadPool and len(ThreadPool) != 0:

        for this_thread in ThreadPool:
            log.error("%s running: %s" % (this_thread._thread_name, this_thread._running_cmd))

        #from Ganga.Core.exceptions import GangaException
        #raise GangaException("Cannot doubbly init the ThreadPool! ThreadPool already populated with threads")
        log.error("Found a thread pool already in existance, wiping it and startig again!")
        del ThreadPool[:]
        ThreadPool = []

    if heartbeat_times is not None:
        heartbeat_times = None

    heartbeat_times = defaultdict( lambda: global_start_time )

    for i in range(THREAD_POOL_SIZE):
        thread_name = "MonitoringWorker_%s_%s" % (str(i), str(int(time.time()*1000)))
        t = MonitoringWorkerThread(name=thread_name)
        ThreadPool.append(t)
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

    global ThreadPool

    def join_worker_threads(threads, timeout=3):
        for t in threads:
            if t.isAlive():
                t.join(timeout)
            t.stop()

    for i in range(len(ThreadPool)):
        Qin.put(JobAction('stop'))

    join_worker_threads(ThreadPool)

    # clean shutdown of stalled threads
    while True:
        if not fail_cb or max_retries <= 0:
            break
        stalled = [t for t in ThreadPool if t.isAlive()]
        if not stalled:
            break
        if fail_cb():  # continue?
            join_worker_threads(stalled, timeout=3)
            max_retries -= 1
        else:
            break

    del ThreadPool[:]
    ThreadPool = []

# purge Qin

def _purge_actions_queue():
    """
    Purge Qin: consume the current queued actions 
    Note: the producer (i.e JobRegistry_Monitor) should be stopped before the method is called
    """
    # purge the queue
    for i in range(len(Qin.queue)):
        try:
            action = Qin.get_nowait()
            # let the *stop* action in queue, otherwise the worker threads will
            # fail to terminate
            if isType(action, JobAction) and action.function == 'stop':
                Qin.put(action)
        except Queue.Empty:
            break

if config['autostart_monThreads'] is True:
    _makeThreadPool()


# Each entry for the updateDict_ts object (based on the UpdateDict class)
# is a _DictEntry object.
class _DictEntry(object):

    def __init__(self, backendObj, jobSet, entryLock, timeoutCounterMax):
        self.backendObj = backendObj
        self.jobSet = jobSet
        self.entryLock = entryLock
        self.timeoutCounterMax = timeoutCounterMax
        self.timeoutCounter = timeoutCounterMax - 0.01
        self.timeLastUpdate = 0.0

    def updateActionTuple(self):
        return self.backendObj, self.jobSet, self.entryLock


@contextmanager
def release_when_done(rlock):
    """
    A ``threading.Lock`` (or ``RLock`` etc.) object cannot be used in a context
    manager if the lock acquisition is non-blocking because the acquisition can
    fail and so the contents of the ``with`` block should not be run.

    To allow sensible ``release()`` behaviour in these cases, acquire the lock
    as usual with ``lock.acquire(False)`` and then wrap the required code with
    this context manager::

        if lock.acquire(blocking=False):
            with release_when_done(lock):
                #some code
                pass

    Attributes:
        rlock: A lock object which can have ``release()`` called on it,
    """
    try:
        yield rlock
    finally:
        rlock.release()


class UpdateDict(object):

    """
    This serves as the Update Table. Is is meant to be used 
    by wrapping it as a SynchronisedObject so as to ensure thread safety.
    """

    def __init__(self):
        self.table = {}

    def addEntry(self, backendObj, backendCheckingFunction, jobList, timeoutMax=None):
        if not jobList:
            return
        if timeoutMax is None:
            timeoutMax = config['default_backend_poll_rate']
        #log.debug("*----addEntry()")

        backend = getName(backendObj)
        if backend in self.table:
            backendObj, jSet, lock = self.table[backend].updateActionTuple()
        else:  # New backend.
            self.table[backend] = _DictEntry(backendObj, set(jobList), threading.RLock(), timeoutMax)
            # queue to get processed
            Qin.put(JobAction(backendCheckingFunction, self.table[backend].updateActionTuple()))
            log.debug("**Adding %s to new %s backend entry." % ([stripProxy(x).getFQID('.') for x in jobList], backend))
            return True

        # backend is in Qin waiting to be processed. Increase it's list of jobs
        # by updating the table entry accordingly. This will reduce the
        # number of update requests.
        # i.e. It's like getting a friend in the queue to pay for your
        # purchases as well! ;p
        log.debug("*: backend=%s, isLocked=%s, isOwner=%s, joblist=%s, queue=%s" % (backend, lock._RLock__count, lock._is_owned(), [x.id for x in jobList], Qin.qsize()))
        if lock.acquire(False):
            try:
                jSetSize = len(jSet)
                log.debug("Lock acquire successful. Updating jSet %s with %s." % ([stripProxy(x).getFQID('.') for x in jSet], [stripProxy(x).getFQID('.') for x in jobList]))
                jSet.update(jobList)
                # If jSet is empty it was cleared by an update action
                # i.e. the queue does not contain an update action for the
                # particular backend any more.
                if jSetSize:  # jSet not cleared
                    log.debug("%s backend job set exists. Added %s to it." % (backend, [stripProxy(x).getFQID('.') for x in jobList]))
                else:
                    Qin.put(JobAction(backendCheckingFunction, self.table[backend].updateActionTuple()))
                    log.debug("Added new %s backend update action for jobs %s." % (backend, [stripProxy(x).getFQID('.') for x in self.table[backend].updateActionTuple()[1]]))

            except Exception as err:
                log.error("addEntry error: %s" % str(err))
            finally:
                lock.release()

            log.debug("**: backend=%s, isLocked=%s, isOwner=%s, joblist=%s, queue=%s" %
                    (backend, lock._RLock__count, lock._is_owned(), [stripProxy(x).getFQID('.') for x in jobList], Qin.qsize()))
            return True

    def clearEntry(self, backend):
        if backend in self.table:
            entry = self.table[backend]
        else:
            log.error("Error clearing the %s backend. It does not exist!" % backend)

        entry.jobSet = set()
        entry.timeoutCounter = entry.timeoutCounterMax

    def timeoutCheck(self):
        for backend, entry in self.table.items():
            # timeoutCounter is reset to its max value ONLY by a successful update action.
            #
            # Initial value and subsequent resets by timeoutCheck() will set the timeoutCounter
            # to a value just short of the max value to ensure that it the timeoutCounter is
            # not decremented simply because there are no updates occuring.
            if entry.timeoutCounter == entry.timeoutCounterMax and entry.entryLock.acquire(False):
                with release_when_done(entry.entryLock):
                    log.debug("%s has been reset. Acquired lock to begin countdown." % backend)
                    entry.timeLastUpdate = time.time()

                    # decrease timeout counter
                    if entry.timeoutCounter <= 0.0:
                        entry.timeoutCounter = entry.timeoutCounterMax - 0.01
                        entry.timeLastUpdate = time.time()
                        log.debug("%s backend counter timeout. Resetting to %s." % (backend, entry.timeoutCounter))
                    else:
                        _l = time.time()
                        entry.timeoutCounter -= _l - entry.timeLastUpdate
                        entry.timeLastUpdate = _l

    def isBackendLocked(self, backend):
        if backend in self.table:
            return bool(self.table[backend].entryLock._RLock__count)
        else:
            return False

    def releaseLocks(self):
        for backend, entry in self.table.items():
            if entry.entryLock._is_owned():
                entry.entryLock.release()


class CallbackHookEntry(object):

    def __init__(self, argDict, enabled=True, timeout=0):
        self.argDict = argDict
        self.enabled = enabled
        # the frequency in seconds
        self.timeout = timeout
        # record the time when this hook has been run
        self._lastRun = 0

def resubmit_if_required(jobList_fromset):

    # resubmit if required
    for j in jobList_fromset:

        if not j.do_auto_resubmit:
            continue

        if len(j.subjobs) == 0:
            try_resubmit = j.info.submit_counter <= config['MaxNumResubmits']
        else:
            # Check for max number of resubmissions
            skip = False
            for s in j.subjobs:
                if s.info.submit_counter > config['MaxNumResubmits'] or s.status == "killed":
                    skip = True

                if skip:
                    continue

                num_com = len([s for s in j.subjobs if s.status in ['completed']])
                num_fail = len([s for s in j.subjobs if s.status in ['failed']])

                #log.critical('Checking failed subjobs for job %d... %d %s',j.id,num_com,num_fail)

                try_resubmit = num_fail > 0 and (float(num_fail) / float(num_com + num_fail)) < config['MaxFracForResubmit']

            if try_resubmit:
                if j.backend.check_auto_resubmit():
                    log.warning('Auto-resubmit job %d...' % j.id)
                    j.auto_resubmit()

    return


def get_jobs_in_bunches(jobList_fromset, blocks_of_size=5, stripProxies=True):
    """
    Return a list of lists of subjobs where each list contains
    a total number of jobs close to 'blocks_of_size' as possible
    whilst not splitting jobs containing subjobs
    Also strip the jobs of their proxies...
    """
    list_of_bunches = []
    temp_list = []

    for this_job in jobList_fromset:

        if stripProxies:
            temp_list.append(stripProxy(this_job))
        else:
            temp_list.append(this_job)

        count = 0
        for found_job in temp_list:
            sj_len = len(found_job.subjobs)
            if sj_len > 0:
                count += sj_len
            else:
                count += 1

        if count >= blocks_of_size:
            list_of_bunches.append(temp_list)
            temp_list = []

    if len(temp_list) != 0:
        list_of_bunches.append(temp_list)
        temp_list = []

    return list_of_bunches


class JobRegistry_Monitor(GangaThread):

    """Job monitoring service thread."""
    uPollRate = 1.
    minPollRate = 1.
    global_count = 0

    def __init__(self, registry_slice):
        GangaThread.__init__(self, name="JobRegistry_Monitor")
        log.debug("Constructing JobRegistry_Monitor")
        self.setDaemon(True)
        self.registry_slice = registry_slice
        self.__sleepCounter = 0.0
        self.__updateTimeStamp = time.time()
        self.progressCallback = lambda x: None
        self.callbackHookDict = {}
        self.clientCallbackDict = {}
        self.alive = True
        self.enabled = False
        # run the monitoring loop continuosly (steps=-1) or just a specified
        # number of steps(>0)
        self.steps = -1
        self.activeBackends = {}
        self.updateJobStatus = None
        self.errors = {}

        self.updateDict_ts = SynchronisedObject(UpdateDict())

        # Create the default backend update method and add to callback hook.
        self.makeUpdateJobStatusFunction()

        # Add credential checking to monitoring loop
        for _credObj in Credentials._allCredentials.itervalues():
            log.debug("Setting callback hook for %s" % getName(_credObj))
            self.setCallbackHook(self.makeCredCheckJobInsertor(_credObj), {}, True, timeout=config['creds_poll_rate'])

        # Add low disk-space checking to monitoring loop
        log.debug("Setting callback hook for disk space checking")
        self.setCallbackHook(self.diskSpaceCheckJobInsertor, {}, True, timeout=config['diskspace_poll_rate'])

        # synch objects
        # main loop mutex
        self.__mainLoopCond = threading.Condition()
        # cleanup synch
        self.__cleanUpEvent = threading.Event()
        # asynch mon loop running synch
        self.__monStepsTerminatedEvent = threading.Event()
        # event to signal the break of job lists iterators
        self.stopIter = threading.Event()
        self.stopIter.set()

        self._runningNow = False

    def isEnabled( self, useRunning = True ):
        if useRunning:
            return self.enabled or self.__isInProgress() and not self.steps
        else:
            return self.enabled and not self.steps

    def run(self):
        """
        Main monitoring loop
        """
        import thread
        from Ganga.Core.MonitoringComponent import monitoring_thread_id
        monitoring_thread_id = thread.get_ident()
        del thread

        log.debug("Starting run method")

        while self.alive:
            checkHeartBeat(JobRegistry_Monitor.global_count)
            log.debug("Monitoring Loop is alive")
            # synchronize the main loop since we can get disable requests
            with self.__mainLoopCond:
                log.debug("Monitoring loop __mainLoopCond")
                log.debug('Monitoring loop lock acquired. Running loop')
                # we are blocked here while the loop is disabled
                while not self.enabled and self.steps <= 0:
                    log.debug("Not enabled")
                    self.__cleanUp()
                    if not self.alive:  # stopped?
                        return
                    # disabled,but still alive, so we keep waiting
                    # for i in range( int(self.uPollRate*20) ):
                    #    if not self.alive:
                    #        return
                    #    self.__mainLoopCond.wait( self.uPollRate*0.05 )
                    self.__mainLoopCond.wait()

                log.debug("Launching Monitoring Step")
                self.__monStep()

                # delay here the monitoring steps according to the
                # configuration
                while self.__sleepCounter > 0.0:
                    log.debug("Wait Condition")
                    self.progressCallback(self.__sleepCounter)
                    for i in range(int(self.uPollRate * 20)):
                        if self.enabled:
                            self.__mainLoopCond.wait(self.uPollRate * 0.05)
                    if not self.enabled:
                        if not self.alive:  # stopped?
                            self.__cleanUp()
                        # disabled, break to the outer while
                        break
                    else:
                        self.__sleepCounter -= self.uPollRate

                else:
                    log.debug("Run on Demand")
                    # run on demand?
                    if self.steps > 0:
                        # decrement the remaining number of steps to run
                        self.steps -= 1
                        # requested number of steps executed, disabling...
                        if self.steps <= 0:
                            self.enabled = False
                            # notify the blocking call of runMonitoring()
                            self.__monStepsTerminatedEvent.set()

        log.debug("Monitoring Cleanup")
        # final cleanup
        self.__cleanUp()

    def __monStep(self):
        """
        A single monitoring step in the monitoring loop
        Note:
        Internally the step does not block, it produces *actions* that are queued to be run 
        in the thread pool
        """
        if not self.callbackHookDict:
            log.error('No callback hooks registered')
            return

        for cbHookFunc in self.callbackHookDict.keys():

            log.debug("\n\nProcessing Function: %s" % cbHookFunc)

            if cbHookFunc in self.callbackHookDict:
                cbHookEntry = self.callbackHookDict[cbHookFunc][1]
            else:
                log.debug("Monitoring KeyError: %s" % str(cbHookFunc))
                continue

            log.debug("cbHookEntry.enabled: %s" % str(cbHookEntry.enabled))
            log.debug("(time.time() - cbHookEntry._lastRun): %s" % str((time.time() - cbHookEntry._lastRun)))
            log.debug("cbHookEntry.timeout: %s" % str(cbHookEntry.timeout))

            if cbHookEntry.enabled and (time.time() - cbHookEntry._lastRun) >= cbHookEntry.timeout:
                log.debug("Running monitoring callback hook function %s(**%s)" %(cbHookFunc, cbHookEntry.argDict))
                #self.callbackHookDict[cbHookFunc][0](**cbHookEntry.argDict)
                try:
                    self.callbackHookDict[cbHookFunc][0](**cbHookEntry.argDict)
                except Exception as err:
                    log.debug("Caught Unknown Callback Exception")
                    log.debug("Callback %s" % str(err))
                cbHookEntry._lastRun = time.time()

        log.debug("\n\nRunning runClientCallbacks")
        self.runClientCallbacks()

        self.__updateTimeStamp = time.time()
        self.__sleepCounter = config['base_poll_rate']

    def runMonitoring(self, jobs=None, steps=1, timeout=300, _loadCredentials=False):
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

        log.debug("runMonitoring")

        if not isType(steps, int) and steps < 0:
            log.warning("The number of monitor steps should be a positive (non-zero) integer")
            return False

        if not self.alive:
            log.error("Cannot run the monitoring loop. It has already been stopped")
            return False

        # we don not allow the user's request the monitoring loop while the
        # internal services are stopped
        if not Coordinator.servicesEnabled:
            log.error("Cannot run the monitoring loop."
                      "The internal services are disabled (check your credentials or available disk space)")
            return False

        # if the monitoring is disabled (e.g. scripts)
        if not self.enabled:
            # and there are some required cred which are missing
            # (the monitoring loop does not monitor the credentials so we need to check 'by hand' here)
            if _loadCredentials is True:
                _missingCreds = Coordinator.getMissingCredentials()
            else:
                _missingCreds = False
            if _missingCreds:
                log.error("Cannot run the monitoring loop. The following credentials are required: %s" % _missingCreds)
                return False

        #log.debug("jobs: %s" % str(jobs))
        #log.debug("self.__mainLoopCond: %s" % str(self.__mainLoopCond))

        with self.__mainLoopCond:
            log.debug('Monitoring loop lock acquired. Enabling mon loop')
            if self.enabled or self.__isInProgress():
                log.error("The monitoring loop is already running.")
                return False

            if jobs is not None:
                m_jobs = jobs

                # additional check if m_jobs is really a registry slice
                # the underlying code is not prepared to handle correctly the
                # situation if it is not
                from Ganga.GPIDev.Lib.Registry.RegistrySlice import RegistrySlice
                if not isType(m_jobs, RegistrySlice):
                    log.warning('runMonitoring: jobs argument must be a registry slice such as a result of jobs.select() or jobs[i1:i2]')
                    return False

                self.registry_slice = m_jobs
                #log.debug("m_jobs: %s" % str(m_jobs))
                self.makeUpdateJobStatusFunction()

            log.debug("Enable Loop, Clear Iterators and setCallbackHook")
            # enable mon loop
            self.enabled = True
            # set how many steps to run
            self.steps = steps
            # enable job list iterators
            self.stopIter.clear()
            # Start backend update timeout checking.
            self.setCallbackHook(self.updateDict_ts.timeoutCheck, {}, True)

            log.debug("Waking up Main Loop")
            # wake up the mon loop
            self.__mainLoopCond.notifyAll()

        log.debug("Waiting to execute steps")
        # wait to execute the steps
        self.__monStepsTerminatedEvent.wait()
        self.__monStepsTerminatedEvent.clear()

        log.debug("Test for timeout")
        # wait the steps to be executed or timeout to occur
        if not self.__awaitTermination(timeout):
            log.warning("Monitoring loop started but did not complete in the given timeout.")
            # force loops termination
            self.stopIter.set()
            return False
        return True

    def enableMonitoring(self):
        """
        Run the monitoring loop continuously
        """

        if not self.alive:
            log.error("Cannot start monitoring loop. It has already been stopped")
            return False

        with self.__mainLoopCond:
            log.debug('Monitoring loop lock acquired. Enabling mon loop')
            self.enabled = True
            # infinite loops
            self.steps = -1
            # enable job list iterators
            self.stopIter.clear()
            log.debug('Monitoring loop enabled')
            # Start backend update timeout checking.
            self.setCallbackHook(self.updateDict_ts.timeoutCheck, {}, True)
            self.__mainLoopCond.notifyAll()

        return True

    def disableMonitoring(self, fail_cb=None, max_retries=5):
        """
        Disable the monitoring loop
        """

        if not self.alive:
            log.error("Cannot disable monitoring loop. It has already been stopped")
            return False

        was_enabled = self.enabled

        try:
            if self.enabled:
                log.debug("Disabling Monitoring Service")

                # THIS NEEDS TO BE HERE FOR A CLEAN DISABLE OF THE MONITORING L
                self.enabled = False
                # CANNOT DETERMINE IF WE SHOULD CONTINUE WITH EXPENSIVE OUT OF
        except Exception as err:
            log.error("ERROR STOPPING MONITORING THREAD, feel free to force exit")
            # This except is left in incase we decide to add things here which
            # can fail!
            log.error("Err %s" % str(err))

        with self.__mainLoopCond:
            log.debug('Monitoring loop lock acquired. Disabling mon loop')
            self.enabled = False
            self.steps = 0
            self.stopIter.set()

            log.debug('Monitoring loop disabled')
            # wake up the monitoring loop
            self.__mainLoopCond.notifyAll()

        if was_enabled is True and self._runningNow:
            log.info("Some tasks are still running on Monitoring Loop")
            log.info("Please wait for them to finish to avoid data corruption")

        #while self._runningNow is True:
        #    time.sleep(0.5)

        #if was_enabled:
        #    log.info("Monitoring Loop has stopped")

        _purge_actions_queue()
        stop_and_free_thread_pool(fail_cb, max_retries)

        JobRegistry_Monitor.global_count = 0
        return True

    def stop(self, fail_cb=None, max_retries=5):
        """
        Stop the monitoring loop
        Parameters:
         fail_cb : if not None, this callback is called if a retry attempt is needed
        """

        if not self.alive and ThreadPool != []:
            log.warning("Monitoring loop has already been stopped")
            return False
        else:
            self.alive = False

        self.__mainLoopCond.acquire()
        if self.enabled:
            log.info('Stopping the monitoring component...')
            self.alive = False
            self.enabled = False

        try:
            # signal the main thread to finish
            self.steps = 0
            self.stopIter.set()
        except Exception as err:
            log.error("stopIter error: %s" % str(err))
        try:
            # wake up the monitoring loop
            self.__mainLoopCond.notifyAll()
        except Exception as err:
            log.error("Monitoring Stop Error: %s" % str(err))
        finally:
            self.__mainLoopCond.release()
        # wait for cleanup
        self.__cleanUpEvent.wait()
        self.__cleanUpEvent.clear()

        # ---->
        # wait for all worker threads to finish
        #self.__awaitTermination()
        # join the worker threads
        _purge_actions_queue()
        stop_and_free_thread_pool(fail_cb, max_retries)
        ###log.info( 'Monitoring component stopped successfully!' )

        #while self._runningNow is True:
        #    time.sleep(0.5)

        JobRegistry_Monitor.global_count = 0
        return True

    def __cleanUp(self):
        """
        Cleanup function ran in JobRegistry_Monitor thread to disable the monitoring loop
        updateDict_ts.timeoutCheck can hold timeout locks that need to be released 
        in order to allow the pool threads to be freed.
        """

        # cleanup the global Qin
        _purge_actions_queue()
        # release timeout check locks
        timeoutCheck = self.updateDict_ts.timeoutCheck
        if timeoutCheck in self.callbackHookDict:
            self.updateDict_ts.releaseLocks()
            self.removeCallbackHook(timeoutCheck)
        # wake up the calls waiting for cleanup
        self.__cleanUpEvent.set()

    def __isInProgress(self):
        if getNumAliveThreads() > 0:
            for this_thread in ThreadPool:
                log.debug("Thread currently running: %s" % str(this_thread._running_cmd))
        return self.steps > 0 or Qin.qsize() > 0 or getNumAliveThreads() > 0

    def __awaitTermination(self, timeout=5):
        """
         Wait for resources to be cleaned up (threads,queue)
         Returns:
             False, on timeout
        """
        while self.__isInProgress():
            time.sleep(self.uPollRate)
            timeout -= self.uPollRate
            if timeout <= 0:
                return False
        return True

    def setCallbackHook(self, func, argDict, enabled, timeout=0):
        func_name = getName(func)
        log.debug('Setting Callback hook function %s.' % func_name)
        log.debug('arg dict: %s' % str(argDict))
        if func_name in self.callbackHookDict:
            log.debug('Replacing existing callback hook function %s with %s' % (str(self.callbackHookDict[func_name]), func_name))
        self.callbackHookDict[func_name] = [func, CallbackHookEntry(argDict=argDict, enabled=enabled, timeout=timeout)]

    def removeCallbackHook(self, func):
        func_name = getName(func)
        log.debug('Removing Callback hook function %s.' % func_name)
        if func_name in self.callbackHookDict:
            del self.callbackHookDict[func_name]
        else:
            log.error('Callback hook function does not exist.')

    def enableCallbackHook(self, func):
        func_name = getName(func)
        log.debug('Enabling Callback hook function %s.' % func_name)
        if func_name in self.callbackHookDict:
            self.callbackHookDict[func_name][1].enabled = True
        else:
            log.error('Callback hook function does not exist.')

    def disableCallbackHook(self, func):
        func_name = getName(func)
        log.debug('Disabling Callback hook function %s.' % func_name)
        if func_name in self.callbackHookDict:
            self.callbackHookDict[func_name][1].enabled = False
        else:
            log.error('Callback hook function does not exist.')

    def runClientCallbacks(self):
        for clientFunc in self.clientCallbackDict:
            log.debug('Running client callback hook function %s(**%s).' % (clientFunc, self.clientCallbackDict[clientFunc]))
            clientFunc(**self.clientCallbackDict[clientFunc])

    def setClientCallback(self, clientFunc, argDict):
        log.debug('Setting client callback hook function %s(**%s).' % (clientFunc, argDict))
        if clientFunc in self.clientCallbackDict:
            self.clientCallbackDict[clientFunc] = argDict
        else:
            log.error("Callback hook function not found.")

    def removeClientCallback(self, clientFunc):
        log.debug('Removing client callback hook function %s.' % clientFunc)
        if clientFunc in self.clientCallbackDict:
            del self.clientCallbackDict[clientFunc]
        else:
            log.error("%s not found in client callback dictionary." % getName(clientFunc))

    def __defaultActiveBackendsFunc(self):
        log.debug("__defaultActiveBackendsFunc")
        active_backends = {}
        # FIXME: this is not thread safe: if the new jobs are added then
        # iteration exception is raised
        fixed_ids = self.registry_slice.ids()
        #log.debug("Registry: %s" % str(self.registry_slice))
        log.debug("Running over fixed_ids: %s" % str(fixed_ids))
        for i in fixed_ids:
            try:
                j = stripProxy(self.registry_slice(i))

                job_status = lazyLoadJobStatus(j)

                if job_status in ['submitted', 'running'] or (j.master and (job_status in ['submitting'])):
                    if self.enabled is True and self.alive is True:
                        backend_obj = lazyLoadJobBackend(j)
                        backend_name = getName(backend_obj)
                        active_backends.setdefault(backend_name, [])
                        active_backends[backend_name].append(j)
            except RegistryKeyError as err:
                log.debug("RegistryKeyError: The job was most likely removed")
                log.debug("RegError %s" % str(err))
            except RegistryLockError as err:
                log.debug("RegistryLockError: The job was most likely removed")
                log.debug("Reg LockError%s" % str(err))

        summary = '{'
        for backend, these_jobs in active_backends.iteritems():
            summary += '"' + str(backend) + '" : ['
            for this_job in these_jobs:
                summary += str(stripProxy(this_job).id) + ', '#getFQID('.')) + ', '
            summary += '], '
        summary += '}'
        log.debug("Returning active_backends: %s" % summary)
        return active_backends

    # This function will be run by update threads
    def _checkBackend(self, backendObj, jobListSet, lock):

        log.debug("\n\n_checkBackend\n\n")

        currentThread = threading.currentThread()
        # timeout mechanism may have acquired the lock to impose delay.
        lock.acquire()
        self._runningNow = True

        try:
            log.debug("[Update Thread %s] Lock acquired for %s" % (currentThread, getName(backendObj)))
            #alljobList_fromset = IList(filter(lambda x: x.status in ['submitted', 'running'], jobListSet), self.stopIter)
            # print alljobList_fromset
            #masterJobList_fromset = IList(filter(lambda x: (x.master is not None) and (x.status in ['submitting']), jobListSet), self.stopIter)

            #FIXME We've lost IList and the above method for adding a job which is in a submitted state looks like one that didn't work
            # Come back and fix this once 6.1.3 is out. We can drop features for functionalist here as the lazy loading is fixed in this release
            # rcurrie
            alljobList_fromset = list(filter(lambda x: x.status in ['submitting', 'submitted', 'running'], jobListSet))
            masterJobList_fromset = list()

            # print masterJobList_fromset
            jobList_fromset = alljobList_fromset
            jobList_fromset.extend(masterJobList_fromset)
            # print jobList_fromset
            self.updateDict_ts.clearEntry(getName(backendObj))
            try:
                log.debug("[Update Thread %s] Updating %s with %s." % (currentThread, getName(backendObj), [x.id for x in jobList_fromset]))

                tested_backends = []

                for j in jobList_fromset:

                    run_setup = False

                    if backendObj is not None:
                        if hasattr(backendObj, 'setup'):
                            stripProxy(j.backend).setup()
                    else:
                        if hasattr(j.backend, 'setup'):
                            stripProxy(j.backend).setup()

                if self.enabled is False and self.alive is False:
                    log.debug("NOT enabled, leaving")
                    return

                block_size = config['numParallelJobs']
                all_job_bunches = get_jobs_in_bunches(jobList_fromset, blocks_of_size = block_size )

                bunch_size = 0
                for bunch in all_job_bunches:
                    bunch_size += len(bunch)
                assert(bunch_size == len(jobList_fromset))

                all_exceptions = []

                for this_job_list in all_job_bunches:

                    if self.enabled is False and self.alive is False:
                        log.debug("NOT enabled, breaking loop")
                        break

                    ### This tries to loop over ALL jobs in 'this_job_list' with the maximum amount of redundancy to keep
                    ### going and attempting to update all (sub)jobs if some fail
                    ### ALL ERRORS AND EXCEPTIONS ARE REPORTED VIA log.error SO NO INFORMATION IS LOST/IGNORED HERE!
                    job_ids = ''
                    for this_job in this_job_list:
                        job_ids += ' %s' % str(this_job.id) 
                    log.debug("Updating Jobs: %s" % job_ids)
                    try:
                        stripProxy(backendObj).master_updateMonitoringInformation(this_job_list)
                    except Exception as err:
                        #raise err
                        log.debug("Err: %s" % str(err))
                        ## We want to catch ALL of the exceptions
                        ## This would allow us to continue in the case of errors due to bad job/backend combinations
                        if err not in all_exceptions:
                            all_exceptions.append(err)

                if all_exceptions != []:
                    for err in all_exceptions:
                        log.error("Monitoring Error: %s" % str(err))
                    ## We should be raising exceptions no matter what
                    raise all_exceptions[0]

                resubmit_if_required(jobList_fromset)

            except BackendError as x:
                self._handleError(x, x.backend_name, 0)
            except Exception as err:
                #self._handleError(err, getName(backendObj), 1)
                log.error("Monitoring Error: %s" % str(err))
                log.debug("Lets not crash here!")
                return

            # FIXME THIS METHOD DOES NOT EXIST
            #log.debug("[Update Thread %s] Flushing registry %s." % (currentThread, [x.id for x in jobList_fromset]))
            # FIXME THIS RETURNS A REGISTRYSLICE OBJECT NOT A REGISTRY, IS THIS CORRECT? SHOULD WE FLUSH
            # COMMENTING OUT AS IT SIMPLY WILL NOT RUN/RESOLVE!
            # this concerns me - rcurrie
            #self.registry_slice._flush(jobList_fromset)  # Optimisation required!

            #for this_job in jobList_fromset:
            #    stripped_job = stripProxy(this_job)
            #    stripped_job._getRegistry()._flush([stripped_job])

        except Exception as err:
            log.debug("Monitoring Loop Error: %s" % str(err))
        finally:
            lock.release()
            log.debug("[Update Thread %s] Lock released for %s." % (currentThread, getName(backendObj)))
            self._runningNow = False

        log.debug("Finishing _checkBackend")
        return

    def _checkActiveBackends(self, activeBackendsFunc):

        log.debug("calling function _checkActiveBackends")
        activeBackends = activeBackendsFunc()

        summary = '{'
        for this_backend, these_jobs in activeBackends.iteritems():
            summary += '"' + this_backend + '" : ['
            for this_job in these_jobs:
                summary += str(stripProxy(this_job).getFQID('.')) + ', '
            summary += '], '
        summary += '}'
        log.debug("Active Backends: %s" % summary)

        for jList in activeBackends.values():

            #log.debug("backend: %s" % str(jList))
            backendObj = jList[0].backend
            b_name = getName(backendObj)
            if b_name in config:
                pRate = config[b_name]
            else:
                pRate = config['default_backend_poll_rate']

            # TODO: To include an if statement before adding entry to
            #       updateDict. Entry is added only if credential requirements
            #       of the particular backend is satisfied.
            #       This requires backends to hold relevant information on its
            #       credential requirements.
            #log.debug("addEntry: %s, %s, %s, %s" % (str(backendObj), str(self._checkBackend), str(jList), str(pRate)))
            self.updateDict_ts.addEntry(backendObj, self._checkBackend, jList, pRate)
            summary = str([stripProxy(x).getFQID('.') for x in jList])
            log.debug("jList: %s" % str(summary))


    def makeUpdateJobStatusFunction(self, makeActiveBackendsFunc=None):
        log.debug("makeUpdateJobStatusFunction")
        if makeActiveBackendsFunc is None:
            makeActiveBackendsFunc = self.__defaultActiveBackendsFunc

        if self.updateJobStatus is not None:
            self.removeCallbackHook(self.updateJobStatus)
        self.updateJobStatus = self._checkActiveBackends
        self.setCallbackHook(self._checkActiveBackends, {'activeBackendsFunc': makeActiveBackendsFunc}, True)
        log.debug("Returning")
        return self.updateJobStatus

    def makeCredCheckJobInsertor(self, credObj):
        def credCheckJobInsertor():
            def cb_Success():
                self.enableCallbackHook(credCheckJobInsertor)

            def cb_Failure():
                self.enableCallbackHook(credCheckJobInsertor)
                self._handleError('%s checking failed!' % getName(credObj), getName(credObj), False)

            log.debug('Inserting %s checking function to Qin.' % getName(credObj))
            _action = JobAction(function=self.makeCredChecker(credObj),
                                callback_Success=cb_Success,
                                callback_Failure=cb_Failure)
            self.disableCallbackHook(credCheckJobInsertor)
            try:
                Qin.put(_action)
            except Exception as err:
                log.debug("makeCred Err: %s" % str(err))
                cb_Failure("Put _action failure: %s" % str(_action), "unknown", True )
        return credCheckJobInsertor

    def makeCredChecker(self, credObj):
        def credChecker():
            log.debug("Checking %s." % getName(credObj))
            try:
                s = credObj.renew()
            except Exception as msg:
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
            self._handleError(
                'Available disk space checking failed and it has been disabled!', 'DiskSpaceChecker', False)

        log.debug('Inserting disk space checking function to Qin.')
        _action = JobAction(function=Coordinator._diskSpaceChecker,
                            callback_Success=cb_Success,
                            callback_Failure=cb_Failure)
        self.disableCallbackHook(self.diskSpaceCheckJobInsertor)
        try:
            Qin.put(_action)
        except Exception as err:
            log.debug("diskSp Err: %s" % str(err))
            cb_Failure()

    def updateJobs(self):
        if time.time() - self.__updateTimeStamp >= self.minPollRate:
            self.__sleepCounter = 0.0
        else:
            self.progressCallback("Processing... Please wait.")
            log.debug(
                "Updates too close together... skipping latest update request.")
            self.__sleepCounter = self.minPollRate

    def _handleError(self, x, backend_name, show_traceback):
        def log_error():
            log.error('Problem in the monitoring loop: %s', str(x))
            #if show_traceback:
            #    log.error("exception: ", exc_info=1)
            #    #log_unknown_exception()
            #    import traceback
            #    traceback.print_stack()
            if show_traceback:
                log_user_exception(log)
        bn = backend_name
        self.errors.setdefault(bn, 0)
        if self.errors[bn] == 0:
            log_error()
            if not config['repeat_messages']:
                log.info('Further error messages from %s handler in the monitoring loop will be skipped.' % bn)
        else:
            if config['repeat_messages']:
                log_error()
        self.errors[bn] += 1


######## THREAD POOL DEBUGGING ###########
def _trace(frame, event, arg):
    setattr(threading.currentThread(), '_frame', frame)


def getStackTrace():
    import inspect

    try:
        status = "Available threads:\n"

        for worker in ThreadPool:
            status = status + "  " + worker.getName() + ":\n"

            if hasattr(worker, '_frame'):
                frame = worker._frame
                if frame:
                    status = status + "    stack:\n"
                    for frame, filename, line, function_name, context, index in inspect.getouterframes(frame):
                        status = status + "      " + function_name + " @ " + filename + " # " + str(line) + "\n"

            status = status + "\n"
        ## CANNOT CONVERT TO A STRING!!!
        #log.info("Queue", str(Qin.queue))
        log.debug("Trace: %s" % str(status))
        return status
    except Exception, err:
        print("Err: %s" % str(err))
    finally:
        pass

