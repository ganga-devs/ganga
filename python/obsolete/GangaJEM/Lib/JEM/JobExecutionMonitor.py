"""
This is the main GPI module of the Ganga integration of the JobExecutionMonitor (JEM).
It defines the GangaObject-derivate JEM uses as an interface for the Ganga-user. She can
access JEMs methods by j.info.monitor.<method>. Those calls get delegated into the JEM
library loaded by the JEMloader object.

@author: Martin Rau, Tim Muenchen
@date: 04.08.09
@organization: University of Wuppertal,
               Faculty of mathematics and natural sciences,
               Department of physics.
@copyright: 2007-2009, University of Wuppertal, Department of physics.
@license: ::

        Copyright (c) 2007-2009 University of Wuppertal, Department of physics

    Permission is hereby granted, free of charge, to any person obtaining a copy of this
    software and associated documentation files (the "Software"), to deal in the Software
    without restriction, including without limitation the rights to use, copy, modify, merge,
    publish, distribute, sublicense, and/or sell copies of the Software, and to permit
    persons to whom the Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all copies
    or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
    INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
    PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
    LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
    TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
    OR OTHER DEALINGS IN THE SOFTWARE.
"""
import sys

if __name__ == '__main__':
    print "Error: This module is not meant to be launched from the command line"
    sys.exit(1)

import os, os.path, getpass, signal, stat, time, re, socket, pprint, select

import PrettyStrings

import Ganga.Utility.Config
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Base import GangaObject

from Ganga.GPIDev.Lib.File import File
from Ganga.Utility.logging import getLogger, logging
from Ganga.Utility.Config import makeConfig, getConfig, ConfigError
from Ganga.Core.GangaThread import GangaThread


########################################################################################################################
# Our logging instance.
logger = getLogger("GangaJEM.Lib.JEM")
outlogger = getLogger("GangaJEM.Lib.JEM.info")

########################################################################################################################
# JEM global configuration options.
jemconfig = makeConfig('JEM', 'Configuration parameters for the Job Execution Monitor')

jemconfig.addOption('JEM_ENABLE', True,
                    "Set this to 'False' to globally disable the JEM monitoring, overriding the monitoring-settings of Job objects.")
jemconfig.addOption('JEM_ENABLE_REALTIME', True,
                    "Set this to 'False' to globally disable realtime monitoring. Monitoring data will only be available in each job's output sandbox.")
jemconfig.addOption('JEM_ENABLE_CTRACE', True,
                    "Set this to 'False' to globally disable c/c++ module tracing.")
jemconfig.addOption('JEM_REPACK', False,
                    'Wether to repack the JEM library before each job submission. This is useful mostly for developers.')
jemconfig.addOption('JEM_MONITOR_SUBJOBS_FREQ', 10000,
                    'Enable JEM monitoring only for every N-th subjob of a splitjob.')
jemconfig.addOption('JEM_VERBOSE_LOADER_DEBUG', False,
                    'Enable verbose debugging output of JEM external library loading.')

#####################################################################################################################################################
# Global initialisation

# JEMloader is a proxy object trying to locate the JEM library
# and insert its path into the python-path. If this does not
# succeed, JEMloader.INITIALIZED will be False afterwards.
from GangaJEM.Lib.JEM import JEMloader

# If JEM was disabled in Ganga, don't go on further...
if not jemconfig['JEM_ENABLE']:
    logger.debug("JEM monitor is globally disabled")
    JEMloader.INITIALIZED = False

# If everything is OK, import the core JEM modules.
if JEMloader.INITIALIZED:
    try:
        from Common.Config import Config as JEM3Config
        from Common.Utils.CoreUtils import unescape_jmd, log_last_exception
        from Common.Utils.Plotter import Plotter
        from Common.Utils import Uuid as uuid
    except:
        logger.debug("Something went wrong when importing JEMs core modules:")
        logger.debug(str(sys.exc_info()[0]) + ": " + str(sys.exc_info()[1]))
        JEMloader.INITIALIZED = False

    from GangaJEM import library #@UnresolvedImport

    try:
        # this is a HACK to pull all config options off of JEM and create GangaObject-representations of them.
        from Modes.Ganga import ConfigConverter
        definition, objlist = ConfigConverter.JEMConfig2GangaObjectSchemas()
        exec(definition, globals(), locals())
    except:
        logger.debug("Failed to inject JEMs config into GangaJEM")
        log_last_exception(logger.debug, True)

# set to True to debug
LOG_STACK_TRACES = True


class JobExecutionMonitor(GangaObject):
    """JEM - The Job Execution Monitor - enables realtime user job monitoring.

    This object is the interface to JEM monitoring. It is a part of the jobinfo-object of a job.

    To activate monitoring, create an instance: j.info.monitor = JobExecutionMonitor(). Then,
    set 'enabled' to True (j.info.monitor.enabled = True). To receive monitoring data in (nearly)
    realtime, set 'realtime' to True. Note that both of this per-job-settings might be overridden
    to 'False' globally in the .gangarc-file.

    Advanced options are available via the config subobject 'advanced'. You can access these
    configuration options with "j.info.monitor.advanced". In most cases, the default settings of
    these options are suffient; please read the documentation of JEM before changing these options.
    Type help(JEMAdvancedOptions) for a detailled description.

    Note that when using splitjobs, by default JEM is enabled only for every 100th subjob to
    prevent monitoring data flooding. You can change this behaviour in .gangarc.

    Methods of interest for the everyday-usage of JEM are:

    getStatus()                  prints status information about the monitored job
    getMetrics()                 prints current WN metrics (load, RAM usage, disk space, etc)
    listExceptions()             lists the last some exceptions that happened
    listCommands()               lists the last some commands / calls / returns that happened
    listAll()                    lists the last some events that happened
    details()                    shows details about one event (exception, command, ...)
    peek()                       peeks into the job's stdout/-err streams (like 'tail')
    livePeek()                   prints the job's stdout as it is created (like 'tail -f')
    getStatistics()              prints statistics about the job's monitoring (e.g. events per sec)
    extractLogfiles()            extracts JEMs logfiles (available after the job finished)
    waitForRealStart()           waits until the the user application on the WN has started
                                 (this wait may be aborted by pressing <return>)

    You can get further help on each of those methods by typing e.g.

        'help(JobExecutionMonitor.getStatus)'.


    NOTE: This is an ALPHA version of JEM. If you have any comments, suggestions or encounter a
          bug, please don't hesitate to give feedback! Visit our webpage for contact information.

    See also: https://svn.grid.uni-wuppertal.de/trac/JEM

    JEM (c)2004-2010 Bergische Universitaet Wuppertal

    """
    _schema = Schema(Version(0,313), {
        'anonymous'              : SimpleItem(hidden=True, defvalue=False, doc='internal anonymize flag'),
        'enabled'                : SimpleItem(defvalue=True, doc='Enables JEM monitoring for the job'),
        'realtime'               : SimpleItem(defvalue=False, doc='Enables realtime reception of monitoring data'),
        'advanced'               : ComponentItem('JEMAdvancedOptions', summary_print='_summary_print',\
                                                 doc='Advanced configuration'),
        'jobID'                  : SimpleItem(hidden=True, defvalue=None, protected=1, copyable=0, transient=1,\
                                              typelist=['type(None)', 'type(str)'],\
                                              doc='Real backend-jobID (or generated)'),
        'andJobIDs'              : SimpleItem(hidden=True, defvalue=None, protected=1, copyable=0, transient=1,\
                                              typelist=['type(None)', 'type(str)'],\
                                              doc='Further backend-jobIDs to monitor (eg. subjobs)'),
        'userAppRunning'         : SimpleItem(hidden=True, defvalue=False, protected=1, copyable=0,\
                                              doc='Has the user application on the WN started running yet?'),
        'userAppExited'          : SimpleItem(hidden=True, defvalue=False, protected=1, copyable=0,\
                                              doc='Has the user application on the WN finished yet?'),
        # internal transient handlers
        'ui'                     : SimpleItem(hidden=True, defvalue=None, protected=1, copyable=0, transient=1,\
                                              typelist=['type(None)', 'UI'], optional=1,\
                                              doc='Internal UI core reference'),
        'chunkProcessor'         : SimpleItem(hidden=True, defvalue=None, protected=1, copyable=0, transient=1,\
                                              typelist=['type(None)', 'GangaChunkProcessor'], optional=1,\
                                              doc='Internal event processor reference'),
        'launchingListener'      : SimpleItem(hidden=True, defvalue=False, protected=1, copyable=0, transient=1,\
                                              doc='Internal flag'),
        # internal identifiers
        'pid'                    : SimpleItem(hidden=True, defvalue=0, protected=1, copyable=0,\
                                              doc='Process id of the job listener'),
        'shmKey'                 : SimpleItem(hidden=True, defvalue=0, protected=1, copyable=0,\
                                              doc='SharedMemory key of the UI core'),
    })

    _category = 'monitor'           # allow insertion into Job.JobInfo object
    _name = 'JobExecutionMonitor'   # GPI-public classname

    # methods accessible from GPI
    _exportmethods = [  # user methods
                        'getStatus', 'getMetrics', 'details',
                        'listAll', 'listExceptions', 'listCommands',
                        'peek', 'outPeek', 'errPeek', 'livePeek',
                        'waitForRealStart',
                        'launchGUI',
                        # debug methods
                        'getStatistics',
                        'getListenerLog',
                        # internal methods (normally not intended to be called by the user)
                        'extractLogfiles',
                        '_getListenerPid',
                        '_getShmKey',
                        '_ensure_listener_running',
                        '_shutdown_listener',
                        '_think',
                        '_register_processor',
                        '_deregister_processor',
                        '_getDebugStatusLine'
                     ]
    
    
    def __init__(self):
        GangaObject.__init__(self)
        try:
            def keyPressed():
                return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])

            p = os.path.expanduser("~/.GangaJEM")
            if not os.path.exists(p):
                os.mkdir(p)

                timeout = 60

                print "* "
                print "* Thank you for trying this beta version of the Job Execution Monitor!"
                print "* "
                print "* This version brings a major rewrite of JEMs worker node module, featur-"
                print "* ing better performance & stability; not all features have yet been por-"
                print "* ted to this new module, though. Refer to the documentation for more in-"
                print "* formation about this: help('JobExecutionMonitor')"
                print "* "
                print "* We'd like to encourage you to give any feedback you have - positive as"
                print "* well as negative - about JEM, to help us improve the system. So if you"
                print "* have anything to comment, don't hesitate to contact us:"
                print "* "
                print "*     send an e-mail to: muenchen@physik.uni-wuppertal.de"
                print "*     visit our homepage https://svn.grid.uni-wuppertal.de/trac/JEM"
                print "* "
                print "* Please note that for statistical purposes, information about all JEM"
                print "* runs is gathered centrally. If you don't wish personalized data (time-"
                print "* stamps, the CEs your jobs get assigned to, the name of the VO, and your"
                print "* grid certificate's subject / your name) to be recorded, please state so:"
                print "* "
                print "*   Allow JEM to gather personalized data?"
                print "* "
                print "*   press return to accept, ctrl+c to deny (defaulting to 'yes' in %d sec)" % timeout

                if keyPressed():
                    sys.stdin.readlines()
                ts = time.time()
                try:
                    while True: # abort condition is in-loop...
                        while time.time() - ts < 1.0:
                            if keyPressed():
                                sys.stdin.read(1)
                                timeout = 0
                                break
                            time.sleep(0.01)
                        if timeout == 0:
                            print "*   ...yes. will gather information about your job runs."
                            break
                        timeout = timeout - 1
                        ts = time.time()
                except KeyboardInterrupt:
                    fd = open(p + "/anonymous", "w")
                    fd.write(".\n")
                    fd.close()
                    self.anonymous = True
                    print "*   ...no. statistics about your job runs will be anonymized."
                except:
                    pass
            else:
                if os.path.exists(p + "/anonymous"):
                    self.anonymous = True
        except:
            # if our check fails, assume 'yes' (GangaRobot et al).
            self.anonymous = False
    
    
    ####################################################################################################################
    ### public interface (methods exported via _exportmethods)
    
    
    def getStatus(self):
        """
        This method prints basic information about the running job, like its job-id
        and the worker node the job is running on.
        """
        try:
            outlogger.info(library.getStatus(self.getJobObject()))
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def launchGUI(self):
        """
        """
        try:
            library.launchGUI(self.getJobObject())
        except:
            logger.error("failed to launch the log explorer GUI - please check the availability of QT4 and PyQT4 on this machine, and enable X-forwarding.")
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def getMetrics(self):
        """
        This method prints information about system the job runs on, like the CPU usage,
        network traffic, and free disk space
        """
        try:
            outlogger.info(library.getMetrics(self.getJobObject()))
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def listAll(self, n = 20, start = 1, ascending = False, exclude=[], include=[]):
        """
        This method lists all monitoring events.

        @param n: how many events to list. default = 5
        @param start: the list begins at the start-th event. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        @param exclude: list of event types to skip. default = []
        @param include: list of event types to list. default = all
        """
        try:
            outlogger.info(library.listAll(self.getJobObject(), n, start, ascending, exclude, include))
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def listExceptions(self, n = 20, start = 1, ascending = False):
        """
        This method lists exceptions happened during the job run. For details about
        an exception, see showException().

        @param n: how many exceptions to list. default = 5
        @param start: the list begins at the start-th exception. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        """
        try:
            outlogger.info(library.listExceptions(self.getJobObject(), n, start, ascending))
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def listCommands(self, n = 20, start = 1, ascending = False):
        """
        This method lists commands executed during the job run. For details about
        a command, see showCommand(). Commands can be commands executed in a script,
        function calls or function returns.

        @param n: how many commands to list. default = 5
        @param start: the list begins at the start-th command. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        """
        try:
            outlogger.info(library.listCommands(self.getJobObject(), n, start, ascending))
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def details(self, event = None):
        """
        This method prints detailled information about a monitoring
        event. The event to view can be specified by its ID or its
        timestamp.

        @param event: Which event to show (ID or timestamp)
        """
        if event is None:
            return
        
        try:
            s = library.showEventDetails(self.getJobObject(), event)
            if s is None:
                return
            outlogger.info(s)
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def peek(self, n = 20, start = 1, ascending = False, with_stdout=True, with_stderr=True):
        """
        This method peeks into the job's output in almost real-time. Output can
        be stdout or stderr-output. Note: The peek-option of JEM must be acti-
        vated, and peeking is not always possible!

        @param n: how many lines to list. default = 20
        @param start: the list begins at the start-th line. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        @param with_stdout: wether to include stdout-lines
        @param with_stderr: wether to include stderr-lines
        """
        try:
            outlogger.info(library.peek(self.getJobObject(), n, start, ascending, with_stdout, with_stderr))
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def outPeek(self, n = 20, start = 1, ascending = False):
        """
        This method peeks into the job's output in almost real-time.

        @param n: how many lines to list. default = 20
        @param start: the list begins at the start-th line. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        """
        try:
            outlogger.info(library.peek(self.getJobObject(), n, start, ascending, True, False))
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def errPeek(self, n = 20, start = 1, ascending = False):
        """
        This method peeks into the job's error-output in almost real-time.

        @param n: how many lines to list. default = 20
        @param start: the list begins at the start-th line. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        """
        try:
            outlogger.info(library.peek(self.getJobObject(), n, start, ascending, False, True))
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def getStatistics(self):
        """
        This method prints statistics about this job's monitoring data.
        """
        try:
            library._checkAndMaybeImportData(self.getJobObject())
            for l in self.chunkProcessor.getStatistics():
                print l
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def livePeek(self):
        """
        This method provides a "tail -f"-like peek into the job's stdout/stderr.
        """
        try:
            library.livePeek(self)
        except:
            log_last_exception(logger.debug, LOG_STACK_TRACES)
    
    
    def extractLogfiles(self):
        """
        Extract the stdout.gz and stderr.gz for the user
        """
        # TODO better error handling :)

        job = self.getJobObject()
        path = job.getOutputWorkspace().getPath() # pylint: disable-msg=E1101

        extractedSome = False

        if self.__decompressTar(path):
            logger.info("Monitoring data of job #" + str(job.id) + " has been extracted, ready for peek()-ing.")
            extractedSome = True
        else:
            logger.info("Monitoring data of job #" + str(job.id) + " couldn't be extracted - this need not be an error (e.g. for split jobs this is normal)")

        for sj in job.subjobs:
            path = sj.getOutputWorkspace().getPath()
            if self.__decompressTar(path):
                logger.info("Monitoring data of job #" + str(job.id) + "." + str(sj.id) + " has been extracted")
            else:
                logger.info("Monitoring data of job #" + str(job.id) + "." + str(sj.id) + " couldn't be extracted")
            extractedSome = True

        return extractedSome
    
    
    def getListenerLog(self):
        """
        Display the logfile of JEM's Listener process (for debugging purposes).
        The Listener is the component receiving the monitoring data in nearly-
        real-time; so if 'realtime' is False, no Listener is launched.
        """
        # not the finest solution, but it works and is quite fast!
        job = self.getJobObject()
        try:
            os.system("less -SR " + job.outputdir + os.sep + "JEM.listener.log")
        except:
            outlogger.info("No live monitor log available")
            outlogger.debug("cause: " + str(sys.exc_info()[0]) + ": " + str(sys.exc_info()[1]))
    
    
    def waitForRealStart(self):
        """
        This method waits for the user application to start on the WN; it may be aborted
        with <return>.
        """
        if self.userAppExited:
            outlogger.info("The user application on worker node already finished running")
            return
        
        def keyPressed():
            return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])
        
        try:
            print "waiting for user application to start... (to abort just press <return>)"
            while not self.userAppRunning and not keyPressed():
                time.sleep(0.5)
        except:
            pass
    
    
    ####################################################################################################################
    ### methods that are exported to GPI, but usually not called by the user...
    
    
    def _getListenerPid(self):
        """
        Return the PID of the event listener process associated with this job, if any
        """
        return self.pid
    
    
    def _getShmKey(self):
        """
        Return the key of the shared memory segment associated with this job, if any
        """
        return str(self.shmKey)
    
    
    def _ensure_listener_running(self):
        """
        Check that there is an event listener process associated with this job and that
        it is running; launch a new one otherwise.
        """
        #logger.debug("_ensure_listener_running() job %s: launchingListener=%s ui=%s key=0x%x" % \
            #(self.getJobObject().id, str(self.launchingListener), str(self.ui), self.shmKey))
        #if self.jobID is None:
        #    return
        if self.launchingListener:
            # don't try to launch listener multiple times at once!
            return
        
        self.launchingListener = True
        
        try:
            # at first, assure our event processor runs. if it doesn't exist, it will get created.
            new_key = self._register_processor()
            
            # then, assure the listener runs.
            def __listener_not_running(self):
                try:
                    proc_dir = "/proc/%d" % self.pid
                    if not os.path.isdir(proc_dir):
                        return True
                except:
                    return True
                return False
            
            if self.pid == 0:
                logger.info("launching listener for job %d" % self.getJobObject().id)
                self._start_listener()
                
                t0 = time.time()
                while __listener_not_running(self):
                    if time.time() - t0 > 30.0: # yes... on high-load machines, we really need to wait that long (duh!)
                        logger.warning("listener for job %d didn't come up after 30.0 seconds")
                        break
                    time.sleep(0.5)
            elif __listener_not_running(self):
                logger.warning("listener for job %d seems to be down - relaunching listener" % self.getJobObject().id)
                self._start_listener()
                time.sleep(5)
            
            if __listener_not_running(self):
                logger.error("failed to launch listener for job %d" % self.getJobObject().id)
        except:
            log_last_exception(logger.debug, True)
        
        self.launchingListener = False
        
        return new_key
    
    
    def _shutdown_listener(self):
        """
        Shutdown the event listener process associated with this job, if any
        """
        if self.pid != 0:
            if self.pid != -1:
                logger.info("shutting down listener for job %d" % self.getJobObject().id)
                try:
                    os.kill(self.pid, signal.SIGTERM)
                except:
                    pass
            self.pid = 0
    
    
    def _register_processor(self, live=True):
        """
        Create an event processor for this job
        """
        if self.shmKey == 0:
            self.shmKey = 0x42000000 + (self.getJobObject().id * 0x10000) + os.getuid()
            logger.debug("no shm key yet for job %d - generated key 0x%08x" % (self.getJobObject().id, self.shmKey))
        if self.ui is None:
            try:
                from Modes.UI.UI import UI
                self.ui = UI()
                logger.info("launching event processor for job %d" % self.getJobObject().id)
                library.setUI(self.getJobObject(), self.ui, live)
                return self.shmKey
            except:
                logger.warn("failed to launch event processor for job %d" % self.getJobObject().id)
                logger.debug("the underlying error was:")
                log_last_exception(logger.debug, True)
                self.ui = None
        return 0
    
    
    def _deregister_processor(self):
        """
        Remove the event processor for this job
        """
        the_key = 0
        if self.ui is not None:
            logger.info("shutting down event processor for job %d" % self.getJobObject().id)
            self.ui.stop()
            library.setUI(self.getJobObject(), None)
            self.ui = None
        # destroy the shmem and sem associated with this job, if present and if no listener is running
        if self.pid == 0 and self.shmKey != 0:
            the_key = self.shmKey
            try:
                result = os.system("ipcrm -M 0x%08x 2>/dev/null" % self.shmKey)
                if result == 0:
                    logger.debug("destroyed shared memory block of job %d's processor" % self.getJobObject().id)
            except: pass
            try:
                result = os.system("ipcrm -S 0x%08x 2>/dev/null" % self.shmKey)
                if result == 0:
                    logger.debug("destroyed semaphore of job %d's processor" % self.getJobObject().id)
            except: pass
            self.shmKey = 0
        return the_key
    
    
    def _think(self):
        """
        Let the event processor do its job. This is called periodically by the framework.
        """
        if self.ui is not None and self.ui.isSetUp():
            self.ui.think()
    
    
    def _getDebugStatusLine(self):
        j = self.getJobObject()
        
        sta = j.status
        ssta = "unknown" #j.info.monitor._getServerStatus()
        hasstarted = self.userAppRunning
        hasexited = self.userAppExited
        
        if sta == "submitted":
            if ssta == "error":
                s += "\033[0;31m"
            elif ssta in ("waiting","disabled","not yet started","unknown"):
                s += "\033[1;30m"
            elif hasstarted:
                s += "\033[1;32m"
            else:
                s += "\033[0;33m"
        elif sta == "running":
            if ssta == "error":
                s += "\033[0;31m"
            elif ssta in ("waiting","disabled","not yet started","unknown"):
                s += "\033[1;30m"
            elif hasstarted:
                s += "\033[1;32m"
            else:
                s += "\033[0;32m"
        else:
            return ""

        if hasexited:
            sta = "app done"

        rs = "  "
        if hasstarted and not hasexited:
            rs = " *"

        s += "#% 5d %s %s % 6d % 6d % 7d %s" % (j.id, sta.rjust(11), rs,\
                                                self.pid, 0, 0, ssta.rjust(16))
                                                #j.info.monitor._getServerPid(),\
                                                #j.info.monitor._getServerPort(),\

        ## data transfer statistics ###
        #stats = j.info.monitor._getTransmissionStats()
        #if stats != [] and stats["Tc"] != 0:
            #s += "  % 6d % 10d % 6d % 6d % 6d % 6d" % (
                                                        #stats["Tc"],
                                                        #stats["Tb"],
                                                        #stats["Rc"],
                                                        #stats["Ec"],
                                                        #stats["Pc"],
                                                        #stats["Cc"]
                                                      #)
        ####

        s += "\n"

    
    ####################################################################################################################
    ### methods not exported to GPI
    
    
    def _start_listener(self):
        try:
            self.pid = library.launchListener(self.getJobObject(), self.jobID, self.andJobIDs)
        except:
            ei = sys.exc_info()
            logger.debug("failed to start listener. underlying exception:")
            log_last_exception(logger.debug, True)
    
    
    def onBegunToReceiveMonitoringData(self):
        """
        This hook is called as soon as the first data is received for this job.
        """
        self.userAppRunning = True
        job = self.getJobObject()
        jobGangaID = job.id # pylint: disable-msg=E1101
        outlogger.info("Begun to receive monitoring data for job %s" % str(jobGangaID))
    
    
    def onUserAppExited(self):
        self.userAppExited = True
        self.userAppRunning = False
        job = self.getJobObject()
        jobGangaID = job.id # pylint: disable-msg=E1101
        outlogger.info("User application of job " + str(jobGangaID) +\
                        " seems to have finished! Now waiting for the middleware...")
    
    
    def getJobID(self):
        if not self.jobID:
            job = self.getJobObject()
            if job.backend.__class__.__name__ == "Localhost":
                self.jobID = uuid.getUniqueID()
                logger.debug("Detected localhost-backend; created jobID \"" + self.jobID + "\"")
            else:
                if type(job.backend.id) == type([]):
                    if len(job.backend.id) > 1:
                        logger.debug("Multiple backend ids detected: " + str(job.backend.id) + " - I'll just use the 1st... (?)")
                    self.jobID = str(job.backend.id[0])
                else:
                    self.jobID = str(job.backend.id)
        return self.jobID
    
    
    def _summary_print(self, attribute, verbosity):
        class Anonymous:
            def __repr__(self):
                return "..."
        if isinstance(attribute, JEMAdvancedOptions):
            return Anonymous()
        return str(attribute)
    
    
    def __decompressTar(self, logfilePath):
        """
        Decompresses the jem monitoring log files into the job output directory
        """
        logfileName = str(logfilePath) + os.sep + "JEM_LOG.tgz"
        if not os.path.exists(logfileName):
            return False
        else:
            try:
                os.system('cd ' + logfilePath + '; tar -xzf ' + logfileName)
                return True
            except:
                return False


### migration classes ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ## ##

class JEMCTraceOptions(GangaObject):
    """Needed for backwards compatibility (ugh!)"""
    _schema = Schema(Version(0,2), {})
    _category = 'JEMCTraceOptions'
    _name = 'JEMCTraceOptions'
