"""
This is the 'backend' module of the Ganga integration of the JobExecutionMonitor (JEM).
It defines callback methods called by the hooks in Job() via an IMonitoringService imple-
mentation; here, all the real work is done (managing JEMs listener process, etc.). The
IMonitoringService-implementation, on the other hand, is a slim delegator object just
passing the callbacks to here.

@author: Tim Muenchen
@date: 06.08.09
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
import os, sys, re, socket, signal, getpass, time

from Ganga.Utility.logging import getLogger, logging
from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Lib.File import File, FileBuffer
from GangaJEM.Lib.JEM import JobExecutionMonitor

########################################################################################################################
# Our logging instance and configuration
logger = getLogger("GangaJEM.Lib.JEM")

jemconfig = getConfig("JEM")

# JEMloader is a proxy object trying to load JEMs core modules. If this
# does not succeed, JEMloader.INITIALIZED will be False afterwards.
import JEMloader
import PrettyStrings

# still, INITIALIZED may be overridden by GangaJEM enablement config.
if not jemconfig['JEM_ENABLE']:
    JEMloader.INITIALIZED = False

# if INITIALIZED, we may import JEMs modules (they are in the pythonpath then)
if JEMloader.INITIALIZED:
    import JEMlib

    # try to import JEM configs and submit methods
    from JEMlib.conf import JEMSysConfig as SysConfig
    from JEMui.conf import JEMuiSysConfig as JEMConfig
    from JEMlib.conf import JEMConfig as WNConfig
    from JEMui.conf import JEMuiConfig as UIConfig

    # import needed JEM modules
    from JEMlib.utils.ReverseFileReader import ropen
    from JEMlib.utils.DictPacker import multiple_replace
    from JEMlib.utils.FreePortFinder import FreePortFinder
    from JEMlib.utils import Utils
    from JEMlib import VERSION as JEM_VERSION

    from Modes.Ganga import ConfigConverter

########################################################################################################################
########################################################################################################################
class JEMMonitoringServiceHandler(object):

    _freeportfinder = None
    _instances = {}

    try:
        _freeportfinder = FreePortFinder()
    except:
        pass

    def getInstance(job):
        """
        Create / get MSHandler for a specific job
        """
        if not job in JEMMonitoringServiceHandler._instances:
            JEMMonitoringServiceHandler._instances[job] = JEMMonitoringServiceHandler(job)
        return JEMMonitoringServiceHandler._instances[job]
    getInstance = staticmethod(getInstance)

    ################################################################################################

    def __init__(self, job):
        """
        Constructor for the servicehandler. One instance is created per job.
        It holds a reference to the job-object.
        """
        self.__job = job
        self.__httpsListenPort = 0
        self.__monitoredSubjobs = []
        self.__JEMrc = None


    def submitting(self):
        """
        This method is called by Job() via IMonitoringService when the job is about to be
        submitted / started.
        """
        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return

        pass # at the moment, everything is done in prepare()... TBD


    def prepare(self, subjobconfig):
        """
        This method is called by Job() via IMonitoringService when the job is being pre-
        pared by the runtime handler. We can add files to the sandboxes here and change
        the executable to JEMs Workernode-script.
        """
        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return

        logger.debug("Job " + self.__getFullJobId() + " is being prepared.")

        mo = self.__job.info.monitor
        backend = self.__job.backend.__class__.__name__

        if not isinstance(mo, JobExecutionMonitor.JobExecutionMonitor):
            logger.debug("Job " + self.__getFullJobId() + " has no JobExecutionMonitor-instance set.")
            return

        jemInputBox = []
        jemOutputBox = []

        ####################################################
        # INPUT-SANDBOX

        # add JEM's main script to inputsandbox
        executablePath = os.path.realpath(JEMloader.JEM_PACKAGEPATH + os.sep + "JEM.py")
        if not os.path.isfile(executablePath):
            logger.warning("Could not find JEM Submit Executable: '%s'. Disabled JEM monitoring." % executablePath)
            mo.enabled = False
            return
        submitExecutable = File(executablePath)

        # locate packed JEM lib
        if not os.path.exists(JEMloader.JEM_PACKAGEPATH + os.sep + "JEM.library.tgz"):
            logger.info("This seems to be your first job submission with the JobExecutionMonitor enabled.")
            logger.info("Preparing JEM for first-time use...")
            try:
                os.system(JEMloader.JEM_PACKAGEPATH + os.sep + "JEM.py --mode Pack >/dev/null")
            except:
                logger.warn('Failed to prepare JEM library package. Disabled JEM monitoring.')
                mo.enabled = False
                return
        # (re)pack JEM library (if needed)
        elif jemconfig['JEM_REPACK']:
            logger.debug("Repacking JEM library")
            try:
                os.system(JEMloader.JEM_PACKAGEPATH + os.sep + "JEM.py --mode Pack >/dev/null")
            except:
                logger.warn('Could not repack JEM library package. JEM library package may be out of date.')
        # still not available?
        if not os.path.exists(JEMloader.JEM_PACKAGEPATH + os.sep + "JEM.library.tgz"):
            logger.warn('Failed to prepare JEM library package. Disabled JEM monitoring.')
            mo.enabled = False
            return

        # add JEM library to inputbox
        libraryPath = os.path.realpath(JEMloader.JEM_PACKAGEPATH + os.sep + "JEM.library.tgz")
        submitLibrary = File(libraryPath)
        jemInputBox += [submitLibrary]

        ####################################################
        # OUTPUT-SANDBOX

        # add files to the output-sandbox
        jemOutputBox += ["JEM_LOG.tgz"]

        ####################################################
        # apply to JEM-enabled subjobs

        def getPath(executable):
            """
            If the given file is found as-is (because the path is included), return None.
            Else, search the exe in $PATH, and return the path to it if found.
            """
            if os.path.isfile(executable):
                return None
            else:
                paths = os.environ['PATH'].split(os.pathsep)
                for p in paths:
                    f = os.path.join(p, executable)
                    if os.path.isfile(f):
                        return p
            return None

        self.__monitoredSubjobs = []
        try:
            for i, config in enumerate(subjobconfig):
                # we monitor only every n-th subjob, with n = JEM_MONITOR_SUBJOBS_FREQ.
                if (i == 0) or (i % jemconfig['JEM_MONITOR_SUBJOBS_FREQ'] == 0):
                    logger.info("Enabling JEM monitoring for job #" + self.__getFullJobId() + "." + str(i))

                    # remember the subjob-ids of all monitored subjobs.
                    self.__monitoredSubjobs += [i]

                    ###---------------------------------------------------------------------------------
                    # as we're replacing the executable with our wrapper script, the executable probably
                    # has to be seperately put into the input sandbox - let's check if it is located some-
                    # where only findable via $PATH
                    addToBox = True
                    sbPath = "."
                    if isinstance(config.exe, File):
                        sbPath = config.exe.subdir
                        config.exe = config.exe.name
                    p = getPath(config.exe)
                    if p:
                        logger.debug("not adding '%s' to the input sandbox, as it is taken from $PATH" % repr(config.exe))
                        addToBox = False

                    if addToBox:
                        if type(config.exe) == type(""):
                            config.exe = File(config.exe, sbPath)
                        config.inputbox += [config.exe]
                    ###---------------------------------------------------------------------------------

                    # now add JEMs files to the boxes...
                    config.inputbox += jemInputBox
                    config.outputbox += jemOutputBox

                    # ...and set the executable to our main WN script, and the original exe to its 1st argument
                    theArgs = ' '.join(config.args)
                    if theArgs != "":
                        theArgs = " "+theArgs
                    theArgs = ["--script", '"' + config.getExeString() + theArgs + '"']
                    config.args = theArgs
                    config.exe = submitExecutable

                    # export the data of the GangaObject representing JEMs config into the environment
                    for k, v in ConfigConverter.GangaObjectInstances2JEMConfigEnvVars(mo.advanced).iteritems():
                        config.env[k] = str(v)

                    # okay, ALL JEM 0.3 config options will be passed by environment for now. Later, we will use JEM
                    # 0.3 also at UI.
                    config.env["JEM_Global_mode"] = "WN"

                    if backend == "LCG":
                        if self.__job.backend.middleware == "GLITE":
                            config.env["JEM_Global_load_job_id_from"] = "GLITE_WMS_JOBID"
                        elif self.__job.backend.middleware == "EDG":
                            config.env["JEM_Global_load_job_id_from"] = "EDG_WMS_JOBID"
                    elif backend == "Panda":
                        config.env["JEM_Global_load_job_id_from"] = ""

                    if config.env.has_key("JEM_WN_script") and config.env["JEM_WN_script"] == "":
                        del(config.env["JEM_WN_script"])

                    try:
                        if mo.advanced.debug:
                            config.env["JEM_Global_debug"] = "True"
                    except:
                        pass

                    if mo.anonymous:
                        logger.debug("Will anonymize spyware information.")
                        config.env["JEM_WN_anonymize_spy"] = "True"

                    # if we're running at Localhost, inject virtual jobID (d'oh...) - otherwise, we don't know it yet!
                    if backend == "Localhost":
                        config.env["JEM_Global_job_id"] = self.__job.info.monitor.getJobID()

                    # determine if we are an Athena job
                    jobIsAthena = self.__isAthenaJob()

                    # configure the C-Tracer
                    if mo.ctracer.enabled:
                        logger.warning("The C-Tracer is an experimental feature (refer to https://svn.grid.uni-wuppertal.de/trac/JEM for more information)")

                        if jobIsAthena:  # The run application for Athena jobs always is Python!
                            config.env['JEM_CTracer_trace_apps'] = "__find_python__"
    
                        if config.env['JEM_CTracer_trace_apps'] == '' and config.env['JEM_CTracer_trace_modules'] != '':
                            config.env['JEM_CTracer_trace_apps'] = config.env['JEM_CTracer_trace_modules']
                    else:
                        config.env['JEM_CTracer_disable'] = "True"

                    # commit all changes we did to the subjobconfig
                    try:
                        config.processValues()
                    except:
                        import traceback
                        ei = sys.exc_info()
                        logger.error("  error occured while preparing: " + str(ei[0]) + ": " + str(ei[1]))
                        logger.debug("  trace:\n" + "".join(traceback.format_tb(ei[2])))

                    ## DEBUG OUTPUT #################################################################
                    s =  "Config for #" + self.__getFullJobId() + "." + str(i) + ":"                #
                    s += "\n    exe = " + str(config.exe)                                           #
                    s += "\n   args = " + str(config.args)                                          #
                                                                                                    #
                    s += "\n    env = {"                                                            #
                    for k in config.env:                                                            #
                        s += "\n            '" + str(k) + "': '"                                    #
                        s += PrettyStrings.formatString(str(config.env[k]), 80).strip() +  "',"     #
                    s += "\n          }"                                                            #
                                                                                                    #
                    s += "\n inp-sb = ["                                                            #
                    for z,ii in enumerate(config.inputbox):                                         #
                        if isinstance(ii, FileBuffer):                                              #
                            ii = ii.subdir + os.sep + ii.name                                       #
                        s += "\n            '" + str(ii) + "'"                                      #
                        if z < len(config.inputbox)-1:                                              #
                            s += ","                                                                #
                    s += "\n          ]"                                                            #
                                                                                                    #
                    s += "\n out-sb = ["                                                            #
                    for z,ii in enumerate(config.outputbox):                                        #
                        s += "\n            '" + str(ii) + "'"                                      #
                        if z < len(config.outputbox)-1:                                             #
                            s += ","                                                                #
                    s += "\n          ]"                                                            #
                    logger.debug(s)                                                                 #
                    #################################################################################
            logger.debug("Monitored subjobs: " + str(self.__monitoredSubjobs))
        except:
            import traceback
            ei = sys.exc_info()
            logger.error("  error occured while preparing: " + str(ei[0]) + ": " + str(ei[1]))
            logger.debug("  trace:\n" + "".join(traceback.format_tb(ei[2])))


    def submit(self):
        """
        This method is called by Job() via IMonitoringService when the job has been
        submitted.
        """
        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return

        # no action for subjobs...
        if self.__job.master:
            return

        if self.__job.subjobs and len(self.__job.subjobs) > 0:
            # HACK: If we are a split job, we have to wait for the subjobs to be assigned
            #       their backend-id (JobID) before we can start the Job-Listener process
            #       So: Wait in it in an own thread!
            ###-------------------------------------------------------------------------------------#########
            from Ganga.Core.GangaThread import GangaThread                                                  #
            class WaitForJobIDsThread(GangaThread):                                                         #
                def __init__(self, job, callback):                                                          #
                    GangaThread.__init__(self, name='wait_for_jobID_thread')                                #
                    self.__job = job                                                                        #
                    self.__callback = callback                                                              #
                    self.setDaemon(True) # don't hang the main thread                                       #
                                                                                                            #
                def run(self):                                                                              #
                    logger.debug("started thread waiting for subjob-ids of job #" + str(self.__job.id))     #
                    while not self.should_stop():                                                           #
                        if self.__job.subjobs[0].backend.id != "":                                          #
                            logger.debug("Job " + str(self.__job.id) + " has been submitted.")              #
                            self.__callback()                                                               #
                            break                                                                           #
                        else:                                                                               #
                            time.sleep(0.25)    # polling 4 times a sec should suffice...                   #
                    logger.debug("thread waiting for subjob-ids of job #" + str(self.__job.id) + " exits")  #
                    self.unregister()                                                                       #
            ###--------------------------------------------------------------------------------------------##
            self.__waitForJobIDsThread = WaitForJobIDsThread(self.__job, self.__startJobListener)
            self.__waitForJobIDsThread.start()
        else:
            logger.debug("Job " + self.__getFullJobId() + " has been submitted.")
            self.__startJobListener()


    def user_app_start(self):
        """
        This hook is called by the watcher thread as soon as the first data is received
        for this job.
        """
        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return

        self.__userAppRunning = True
        logger.info("Begun to receive monitoring data for job " + str(self.__job.id))


    def complete(self, cause):
        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return

        if self.__job.master: # subjobs
            return
        elif not self.__job.info.monitor or self.__job.info.monitor.__class__.__name__ != "JobExecutionMonitor":
            return
        else:
            # main job
            logger.debug("Job " + self.__getFullJobId() + " has completed. Status: " + cause)
            self.__stopJobListener()
            if cause != "failed":
                if self.__job.info.monitor.extractLogfiles():
                    # now that the job has finished and the logiles have been extracted,
                    # we copy the full JMD log (if present) into our workspace (so all
                    # the events can be inspected from within Ganga)
                    copiedStuff = False

                    if os.path.exists(self.__job.info.monitor.jmdfile):
                        if not os.path.exists(self.__job.outputdir + "JEM_MON.jmd"):
                            logger.info("no log in output sandbox, but live received data is present - copying into output dir")
                            os.system('cp ' + self.__job.info.monitor.jmdfile + ' ' + self.__job.outputdir + "JEM_MON.jmd")
                        try:
                            os.system('mv ' + self.__job.info.monitor.jmdfile + ' ' + self.__job.info.monitor.jmdfile + '.bak')
                        except:
                            pass

                    try:
                        if os.path.exists(self.__job.outputdir + "JEM_MON.jmd"):
                            logger.info("Copying the full log into the workspace...")
                            os.system('cp ' + self.__job.outputdir + "JEM_MON.jmd " + self.__job.info.monitor.jmdfile)
                            copiedStuff = True
                            logger.debug("  OK, log of main job #" + str(self.__job.id) + " has been copied.")
                    except:
                        pass

                    logger.debug("  (now trying to copy the subjobs' logs)")

                    for sj in self.__job.subjobs:
                        if os.path.exists(sj.outputdir + "JEM_MON.jmd"):
                            try:
                                os.system('cp ' + sj.outputdir + "JEM_MON.jmd " + self.__job.info.monitor.jmdfile + ".subjob." + str(sj.id))
                                logger.debug("  OK, log of subjob #" + str(self.__job.id) + "." + str(sj.id) + " has been copied.")
                                copiedStuff = True
                            except:
                                pass

                    if copiedStuff:
                        logger.info("...done. Inspecting the data now should display the whole available info.")

                if not os.path.exists(self.__job.outputdir + "JEM_MON.jmd"):
                    if os.path.exists(self.__job.info.monitor.jmdfile):
                        logger.info("no log in output sandbox, but live received data is present - copying into output dir")
                        os.system('cp ' + self.__job.info.monitor.jmdfile + ' ' + self.__job.outputdir + "JEM_MON.jmd")


    def rollback(self):
        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return

        logger.debug("Job " + self.__getFullJobId() + " got rolled back to new.")
        self.__stopJobListener()


    ################################################################################################

    def __getFullJobId(self):
        jid = str(self.__job.id)
        if self.__job.master:
            jid = str(self.__job.master.id) + "." + jid
        return jid


    def __isAthenaJob(self):
        # pylint: disable-msg=E1101
        try:
            return self.__job.application.__class__.__name__ == "Athena"
        except:
            return False


    def __startJobListener(self):
        """
        Start the job listener for live monitoring support
        """
        if not isinstance(self.__job.info.monitor, JobExecutionMonitor.JobExecutionMonitor):
            return

        jobID = self.__job.info.monitor.getJobID()
        escapedJobID = Utils.escapeJobID(jobID)

        jmdDir = WNConfig.LOG_DIR + os.sep + escapedJobID
        if not os.path.exists(jmdDir):
            os.makedirs(jmdDir, 0777)

        self.__job.info.monitor.jmdfile = jmdDir + os.sep + UIConfig.PUBLISHER_JMD_FILE

        if not self.__job.info.monitor.realtime or not jemconfig['JEM_ENABLE_REALTIME']: # pylint: disable-msg=E1101
            logger.debug('realtime mode is disabled, not launching listener process')
            return

        try:
            # debug output ##############################################################
            logger.debug('Trying to launch JEM realtime monitoring listener process')   #
                                                                                        #
            s = '  for job with id "%s"' % jobID                                        #
            if self.__job.subjobs and len(self.__job.subjobs) > 0:                      #
                s += ' (%d subjobs)' % len(self.__job.subjobs)                          #
            logger.debug(s)                                                             #
                                                                                        #
            for i, sj in enumerate(self.__job.subjobs):                                 #
                s = "  * subjob %d: backend id %s" % (i, str(sj.backend.id))            #
                if i in self.__monitoredSubjobs:                                        #
                    s += " [M]"                                                         #
                logger.debug(s)                                                         #
            #############################################################################

            # the job listener executable now lies in a subdir of the JEM package path.
            executable = JEMloader.JEM_PACKAGEPATH + os.sep + 'legacy' + os.sep + 'JEMganga' + os.sep + 'LiveMonitoring.py'
            try:
                stompserver = self.__job.info.monitor.advanced.stompvalve.host
                stompport = str(self.__job.info.monitor.advanced.stompvalve.port)
                args = [executable, "--stomp-server", stompserver, "--stomp-port", stompport, jobID]
            except:
                args = [executable, jobID]

            # further job IDs (subjob IDs)
            for i, sj in enumerate(self.__job.subjobs):
                if i in self.__monitoredSubjobs:
                    args += [str(sj.backend.id)]

            try:
                self.__job.info.monitor.pid = os.spawnve(os.P_NOWAIT, executable, args, os.environ)
            except Exception, r:
                logger.error('Could not start job listener process.')
                logger.error('The Job with the id %s will start without monitoring.' % jobID)
                logger.error('Error cause: %s.' % str(r))
                if self.pid > 0:
                    self.__stopJobListener()
                self.__job.info.monitor.enabled = False # pylint: disable-msg=E1101
                return

            logger.info('The JEM realtime monitoring listener has been started for job ' + str(jobID) + '.')
            logger.debug('Listener process: PID %s' % self.__job.info.monitor.pid)
            logger.debug('Listener arguments: ' + str(args))
            logger.debug('Logfiles: %s' % WNConfig.LOG_DIR + os.sep + escapedJobID)

            # start a watcher thread for this job.
            self.__job.info.monitor.watch(jobID)
        except:
            ei = sys.exc_info()
            logger.error("An error occured while trying to launch the JEM realtime monitoring listener process.")
            logger.error("The error was: " + str(ei[0]) + ": " + str(ei[1]))


    def __stopJobListener(self):
        """
        Shutdown the job listener of this job
        """
        if not isinstance(self.__job.info.monitor, JobExecutionMonitor.JobExecutionMonitor):
            logger.debug('__stopJobListener(): j.info.monitor is no JobExecutionMonitor!')
            return

        logger.debug('__stopJobListener() [pid %s]: aborting watcher thread' % self.__job.info.monitor.pid)
        self.__job.info.monitor.abortWatch()

        if not self.__job.info.monitor.realtime or not jemconfig['JEM_ENABLE_REALTIME']: # pylint: disable-msg=E1101
            logger.debug('__stopJobListener() [pid %s]: no realtime...' % self.__job.info.monitor.pid)
            return

        logger.debug('__stopJobListener() [pid %s]: shutting down process.' % self.__job.info.monitor.pid)
        logger.info("The JEM realtime monitoring listener for job %s exits." % self.__job.info.monitor.getJobID())
        if self.__job.info.monitor.pid != 0:
            self.__killprocess(self.__job.info.monitor.pid)


    def __getChildProcesses(self):
        """
        Find the child-processes of LiveMonitoring.py (Server, PipePublisher-launched stuff, etc).
        """
        pids = ppids = cmds = []

        _pid_match = re.compile("(?P<pid>[0-9]{1,5})")
        for root, dirs, files in os.walk("/proc"):
            _del = []
            for d in dirs:
                try:
                    m = _pid_match.match(d)
                except:
                    m = None
                if m:
                    pid = m.group("pid")
                    fd = open("/proc/%s/stat" % pid, "r")
                    for l in fd.readlines():
                        ppid = int(l.split()[3])
                        if ppid == self.__job.info.monitor.pid:
                            pids += [int(pid)]
                            ppids += [ppid]
                            fd2 = open("/proc/%s/cmdline" % pid, "r")
                            cmds += [fd2.readline()]
                            fd2.close()
                    fd.close()
                # do not recurse.
                _del += [d]
            for d in _del:
                dirs.remove(d)
        return pids, ppids, cmds


    def __killprocess(self,pid):
        """
        Helpermethod to kill a given process by its own id. First try to kill it with term signal.
        If this doesn't work try to kill it with kill signal, and otherwise by direct os call.
        """
        try:
            pid = int(pid)
        except:
            logger.debug("Invalid PID given to __killprocess: " + str(pid))
            return

        def internal_shutdown(thepid):
            logger.debug('internal_shutdown(%s)' % str(thepid))
            thepid = int(thepid)
            try:
                os.kill(thepid, signal.SIGTERM)
                time.sleep(0.5)
                if (os.waitpid(thepid, os.WNOHANG) == (0,0)):
                    logger.debug('internal_shutdown(%s): -15 did not suffice, killing using -9' % str(thepid))
                    os.kill(thepid, signal.SIGKILL)
            except OSError, ose:
                logger.debug('internal_shutdown(%s): %s' % (str(thepid), str(ose)))
                if "No such process" in str(ose):
                    return
                else:
                    os.system("kill -9 " + str(thepid) + " 2> /dev/null")

        try:
            # first, look if we got childprocesses to kill first:
            pids,ppids,cmds = self.__getChildProcesses() # pylint: disable-msg=W0612
            logger.debug('shutting down LM child processes:')
            for z,p in enumerate(pids):
                if ppids[z] == pid:
                    internal_shutdown(p)
                    logger.debug('   %s: %s' % (str(p), cmds[z]))
            logger.debug('shutting down LM main process (%s)' % str(pid))
            internal_shutdown(pid)
        except:
            ei = sys.exc_info()
            logger.debug("in __killprocess: " + str(ei[0]) + ": " + str(ei[1]))
            # try to kill process with os call, will always work (or not, if the process doesn't exist...)!
            try:
                os.system("kill -9 " + str(pid) + " 2> /dev/null")
            except:
                return

