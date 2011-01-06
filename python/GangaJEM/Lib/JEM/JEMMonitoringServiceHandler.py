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
logger = getLogger("GangaJEM.Lib.JEM.info")

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
    # JEM 0.3 modules
    from Modes.Ganga import ConfigConverter
    from Common.Utils.CoreUtils import job_id_to_stomp_topic


########################################################################################################################
########################################################################################################################
class JEMMonitoringServiceHandler(object):

    _instances = {}

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
        self.__monitoredSubjobs = []
        #logger.info("JEMMonitoringServiceHandler created for job " + str(job))


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

        if mo.enabled == False:
            logger.debug("JEM is not enabled for job " + self.__getFullJobId() + ".")
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
                os.spawnvpe(os.P_WAIT, sys.executable,
                            [sys.executable, JEMloader.JEM_PACKAGEPATH + os.sep + "JEM.py", "--mode", "Packer"], {})
            except:
                logger.warn('Failed to prepare JEM library package. Disabled JEM monitoring.')
                mo.enabled = False
                return
        # (re)pack JEM library (if needed)
        elif jemconfig['JEM_REPACK']:
            logger.debug("Repacking JEM library")
            try:
                os.spawnvpe(os.P_WAIT, sys.executable,
                            [sys.executable, JEMloader.JEM_PACKAGEPATH + os.sep + "JEM.py", "--mode", "Packer"], {})
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
                    logger.info("Enabling JEM monitoring for job " + self.__getFullJobId() + "." + str(i))

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
                            config.env["JEM_Global_loadJobIdFrom"] = "GLITE_WMS_JOBID"
                        elif self.__job.backend.middleware == "EDG":
                            config.env["JEM_Global_loadJobIdFrom"] = "EDG_WL_JOBID"
                    elif backend == "Panda":
                        config.env["JEM_Global_loadJobIdFrom"] = ""

                    if config.env.has_key("JEM_WN_script") and config.env["JEM_WN_script"] == "":
                        del(config.env["JEM_WN_script"])

                    try:
                        if mo.advanced.debug:
                            config.env["JEM_Global_debug"] = "True"
                    except:
                        pass

                    if mo.anonymous:
                        logger.debug("Will anonymize spyware information.")
                        config.env["JEM_WN_anonymizeSpy"] = "True"

                    # if we're running at Localhost, inject virtual jobID (d'oh...) - otherwise, we don't know it yet!
                    if backend == "Localhost":
                        config.env["JEM_Global_jobId"] = self.__job.info.monitor.getJobID()

                    # determine if we are an Athena job
                    jobIsAthena = self.__isAthenaJob()

                    # configure the C-Tracer
                    #if mo.ctracer.enabled:
                        #logger.warning("The C-Tracer is an experimental feature (refer to https://svn.grid.uni-wuppertal.de/trac/JEM for more information)")

                        #if jobIsAthena:  # The run application for Athena jobs always is Python!
                            #config.env['JEM.CTracer.trace_apps'] = "__find_python__"

                        #if config.env['JEM.CTracer.trace_apps'] == '' and config.env['JEM.CTracer.trace_modules'] != '':
                            #config.env['JEM.CTracer.trace_apps'] = config.env['JEM.CTracer.trace_modules']
                    #else:
                        #config.env['JEM.CTracer.disable'] = "True"

                    # add some information about this Ganga session
                    #config.env['JEM.Ganga.Version'] = 
                    config.env['JEM_Ganga_jobId'] = self.__getFullJobId() + "." + str(i)
                    try:
                        from socket import gethostname
                        config.env['JEM_Ganga_submitHost'] = gethostname()
                    except:
                        pass

                    try:
                        from os import getlogin
                        config.env['JEM_Ganga_localUser'] = getlogin()
                    except:
                        pass

                    # commit all changes we did to the subjobconfig
                    try:
                        config.processValues()
                    except:
                        import traceback
                        ei = sys.exc_info()
                        logger.error("  error occured while preparing: " + str(ei[0]) + ": " + str(ei[1]))
                        logger.debug("  trace:\n" + "".join(traceback.format_tb(ei[2])))

                    if False:
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
        if self.__job.master != None:
            return

        mo = self.__job.info.monitor

        if not isinstance(mo, JobExecutionMonitor.JobExecutionMonitor):
            logger.debug("Job " + self.__getFullJobId() + " has no JobExecutionMonitor-instance set.")
            return

        if mo.enabled == False:
            logger.debug("JEM is not enabled for job " + self.__getFullJobId() + ".")
            return

        # nicely ask the user to give some feedback.
        logger.info("* The Job Execution Monitor is active for this job.")
        logger.info("*   Please consider providing (positive and/or negative) feedback of your user experience")
        logger.info("*   with GangaJEM - visit https://svn.grid.uni-wuppertal.de/trac/JEM for that. Thanks :)")

        if self.__job.subjobs and len(self.__job.subjobs) > 0:
            # HACK: If we are a split job, we have to wait for the subjobs to be assigned
            #       their backend-id (JobID) before we can start the Job-Listener process
            #       So: Wait for it in an own thread!
            ###-------------------------------------------------------------------------------------#########
            from Ganga.Core.GangaThread import GangaThread                                                  #
            class WaitForJobIDsThread(GangaThread):                                                         #
                def __init__(self, handler, job, callback):                                                 #
                    GangaThread.__init__(self, name='wait_for_jobID_thread')                                #
                    self.__serviceHandler = handler                                                         #
                    self.__job = job                                                                        #
                    self.__callback = callback                                                              #
                    self.setDaemon(True) # don't hang the main thread                                       #
                                                                                                            #
                def run(self):                                                                              #
                    logger.debug("started thread waiting for subjob-ids of job #" + str(self.__job.id))     #
                    while not self.should_stop():                                                           #
                        all_ids_known = True                                                                #
                        for sj_id in self.__serviceHandler.__monitoredSubjobs:                              #
                            if self.__job.subjobs[i].backend.id == "":                                      #
                                all_ids_known = False                                                       #
                        if all_ids_known:                                                                   #
                            logger.debug("Job " + str(self.__job.id) + " has been submitted.")              #
                            self.__callback()                                                               #
                            break                                                                           #
                        else:                                                                               #
                            time.sleep(0.25)    # polling 4 times a sec should suffice...                   #
                    logger.debug("thread waiting for subjob-ids of job #" + str(self.__job.id) + " exits")  #
                    self.unregister()                                                                       #
            ###--------------------------------------------------------------------------------------------##
            self.__waitForJobIDsThread = WaitForJobIDsThread(self, self.__job, self.__startJobListener)
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
            return
        self.__userAppRunning = True
        logger.info("Begun to receive monitoring data for job " + str(self.__job.id))


    def complete(self, cause):
        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return
        
        mo = self.__job.info.monitor
        
        if self.__job.master != None: # subjobs
            return
        elif not isinstance(mo, JobExecutionMonitor.JobExecutionMonitor):
            return
        elif mo.enabled == False:
            return
        else:
            # main job
            logger.debug("Job " + self.__getFullJobId() + " has completed. Status: " + cause)
            self.__stopJobListener()

            if self.__job.info.monitor.extractLogfiles():
                # now that the job has finished and the logiles have been extracted,
                # we copy the full JMD log (if present) into our workspace (so all
                # the events can be inspected from within Ganga)
                copiedStuff = False

                # TODO

                #if os.path.exists(self.__job.info.monitor.jmdfile):
                    #if not os.path.exists(self.__job.outputdir + "JEM_MON.jmd"):
                        #logger.info("no log in output sandbox, but live received data is present - copying into output dir")
                        #os.system('cp ' + self.__job.info.monitor.jmdfile + ' ' + self.__job.outputdir + "JEM_MON.jmd")
                    #try:
                        #os.system('mv ' + self.__job.info.monitor.jmdfile + ' ' + self.__job.info.monitor.jmdfile + '.bak')
                    #except:
                        #pass

                #try:
                    #if os.path.exists(self.__job.outputdir + "JEM_MON.jmd"):
                        #logger.info("Copying the full log into the workspace...")
                        #os.system('cp ' + self.__job.outputdir + "JEM_MON.jmd " + self.__job.info.monitor.jmdfile)
                        #copiedStuff = True
                        #logger.debug("  OK, log of main job #" + str(self.__job.id) + " has been copied.")
                #except:
                    #pass

                #logger.debug("  (now trying to copy the subjobs' logs)")

                #for sj in self.__job.subjobs:
                    #if os.path.exists(sj.outputdir + "JEM_MON.jmd"):
                        #try:
                            #os.system('cp ' + sj.outputdir + "JEM_MON.jmd " + self.__job.info.monitor.jmdfile + ".subjob." + str(sj.id))
                            #logger.debug("  OK, log of subjob #" + str(self.__job.id) + "." + str(sj.id) + " has been copied.")
                            #copiedStuff = True
                        #except:
                            #pass

                if copiedStuff:
                    logger.info("...done. Inspecting the data now should display the whole available info.")

            #if not os.path.exists(self.__job.outputdir + "JEM_MON.jmd"):
                #if os.path.exists(self.__job.info.monitor.jmdfile):
                    #logger.info("no log in output sandbox, but live received data is present - copying into output dir")
                    #os.system('cp ' + self.__job.info.monitor.jmdfile + ' ' + self.__job.outputdir + "JEM_MON.jmd")


    def rollback(self):
        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return

        logger.debug("Job " + self.__getFullJobId() + " got rolled back to new.")
        self.__stopJobListener()


    ################################################################################################

    def __getFullJobId(self):
        jid = str(self.__job.id)
        if self.__job.master != None:
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
        escapedJobID = job_id_to_stomp_topic(jobID)

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
            
            # further job IDs (subjob IDs) - set this first to avoid race condition (JEMServiceThread waits
            # for monitor.jobID to be set)
            for i, sj in enumerate(self.__job.subjobs):
                if i in self.__monitoredSubjobs:
                    if self.__job.info.monitor.andJobIDs is not None:
                        self.__job.info.monitor.andJobIDs += ',' + str(sj.backend.id)
                    else:
                        self.__job.info.monitor.andJobIDs = str(sj.backend.id)
            
            self.__job.info.monitor.jobID = escapedJobID
            
            # don't launch the listener here, this will collide with the JEMServiceThread.
            #self.__job.info.monitor._ensure_listener_running()
        except:
            ei = sys.exc_info()
            logger.error("An error occured while trying to launch the JEM realtime monitoring listener process.")
            logger.error("The error was: " + str(ei[0]) + ": " + str(ei[1]))


    def __stopJobListener(self):
        """
        Shutdown the job listener of this job
        """
        if not isinstance(self.__job.info.monitor, JobExecutionMonitor.JobExecutionMonitor):
            return
        
        if not self.__job.info.monitor.realtime or not jemconfig['JEM_ENABLE_REALTIME']: # pylint: disable-msg=E1101
            logger.debug('__stopJobListener() [pid %s]: no realtime...' % self.__job.info.monitor.pid)
            return
        
        self.__job.info.monitor._shutdown_listener()
        self.__job.info.monitor.realtime = False
