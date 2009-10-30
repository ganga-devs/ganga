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
import os, sys, re, socket, getpass, time

from Ganga.Utility.logging import getLogger, logging
from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Lib.File import File
from GangaJEM.Lib.JEM import JobExecutionMonitor

########################################################################################################################
# Our logging instance and configuration
logger = getLogger()
logger.setLevel(logging.INFO)
jemconfig = getConfig("JEM")

# JEMloader is a proxy object trying to load JEMs core modules. If this
# does not succeed, JEMloader.INITIALIZED will be False afterwards.
import JEMloader
import PrettyStrings

if not jemconfig['JEM_ENABLE']:
    JEMloader.INITIALIZED = False

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


    def submitting(self):
        """
        This method is called by Job() via IMonitoringService when the job is about to be
        submitted / started.
        """
        pass # at the moment, everything is done in prepare()... TBD


    def prepare(self, subjobconfig):
        """
        This method is called by Job() via IMonitoringService when the job is being pre-
        pared by the runtime handler. We can add files to the sandboxes here and change
        the executable to JEMs Workernode-script.
        """
        logger.debug("Job " + self.__getFullJobId() + " is being prepared.")

        if not isinstance(self.__job.info.monitor, JobExecutionMonitor.JobExecutionMonitor):
            logger.debug("Job " + self.__getFullJobId() + " has no JobExecutionMonitor-instance set.")
            return

        if not JEMloader.INITIALIZED:
            logger.debug("Job Execution Monitor is disabled or failed to initialize")
            return

        jemInputBox = []
        jemOutputBox = []

        ####################################################
        # INPUT-SANDBOX

        # add JEM submit script to inputsandbox
        executablePath = os.path.realpath(JEMloader.JEM_PACKAGEPATH + os.path.sep + JEMConfig.UI_SUBMIT_EXECUTABLE)
        if not os.path.isfile(executablePath):
            logger.warning("Could not find JEM Submit Executable: '%s'. Disabled JEM monitoring." % executablePath)
            self.__job.info.monitor.enabled = False
            return

        submitExecutable = File(executablePath)

        def locateJemLib():
            if not os.path.exists(JEMConfig.UI_SUBMIT_PACKAGE):
                if os.path.exists(JEMConfig.UI_SUBMIT_PACKAGE + '.gz'):
                    return '.gz'
                elif os.path.exists(JEMConfig.UI_SUBMIT_PACKAGE + '.zip'):
                    return '.zip'
                elif os.path.exists(JEMConfig.UI_SUBMIT_PACKAGE + '.bz2'):
                    return '.bz2'
                else:
                    return None
            else:
                return ''

        # locate JEM lib (incl. possible archive extension)
        if locateJemLib() == None:
            logger.info("This seems to be your first job submission with the JobExecutionMonitor enabled.")
            logger.info("Preparing JEM for first-time use...")
            try:
                os.system(JEMConfig.UI_SUBMIT_PACKER + " >/dev/null")
            except:
                logger.warn('Failed to prepare JEM library package. Disabled JEM monitoring.')
                self.__job.info.monitor.enabled = False
                return
        # (re)pack JEM library (if needed)
        elif jemconfig['JEM_REPACK']:
            logger.debug("Repacking JEM library")
            try:
                os.system(JEMConfig.UI_SUBMIT_PACKER + " >/dev/null")
            except:
                logger.warn('Could not repack JEM library package. JEM library package may be out of date.')

        ending = locateJemLib()
        if ending == None:
            logger.warn('Failed to prepare JEM library package. Disabled JEM monitoring.')
            self.__job.info.monitor.enabled = False
            return
        JEMConfig.UI_SUBMIT_PACKAGE += ending

        # add JEM library to inputbox
        libraryPath = os.path.realpath(JEMConfig.UI_SUBMIT_PACKAGE)
        submitLibrary = File(libraryPath)
        jemInputBox += [submitLibrary]

        # add modified .JEMrc
        # we will try to load the .JEMrc settings in a dict first.
        # this is the dict:
        theJEMrcSettings = {}
        # its format is { "section" => { "key" => "value", ... } }

        # first, look for the .JEMrc in the current path...
        localConfigPath = os.path.realpath("." + os.path.sep + ".JEMrc")
        if not os.path.isfile(localConfigPath):
            # ...if it's not there, look for ~/.JEMrc...
            localConfigPath = os.path.expanduser("~") + os.path.sep + ".JEMrc"
            if not os.path.isfile(localConfigPath):
                # ...if that also doesn't exist, we'll just create a new one and warn the user.
                logger.warn("Didn't find local configuration file .JEMrc in "
                               + os.path.realpath("." + os.path.sep + ".JEMrc") + " or "
                               + os.path.expanduser("~") + os.path.sep + ".JEMrc")
                logger.warn("JEM will run with the default settings set up by the site administrator."
                               + " In most cases, this means no realtime monitoring data will be available.")

        # if a .JEMrc was found, load its settings in our dict.
        if os.path.isfile(localConfigPath):
            logger.debug("Read JEM user settings from " + localConfigPath)
            theJEMrcSettings = self.__readJEMrc(localConfigPath)

        # modify the settings to reflect our Ganga-JEM-settings...
        self.__modifyJEMrcSettings(theJEMrcSettings)

        # ...write a new .JEMrc in the temp dir...
        jemRCFile = self.__writeJEMrc(theJEMrcSettings)

        # ...and add it to the input-sandbox
        submitLocalConfig = File(jemRCFile)
        jemInputBox += [submitLocalConfig]

        ####################################################
        # OUTPUT-SANDBOX

        # add files to the output-sandbox
        jemOutputBox += [WNConfig.COMPLETE_LOG]

        ####################################################
        # listener-port-handling

        import sys

        if self.__job.info.monitor.realtime:
            try:
                h,p = WNConfig.PUBLISHER_HTTPS_SERVER.split(":")
                self.__httpsListenPort = int(p)
            except:
                self.__httpsListenPort = 0

            if self.__httpsListenPort == 0:
                # try to find a free listening port for the HTTPSServer
                self.__httpsListenPort = self.__tryFindFreeListeningPort()

        def getPath(executable):
            paths = os.environ['PATH'].split(os.pathsep)
            if os.path.isfile(executable):
                return None
            else:
                for p in paths:
                    f = os.path.join(p, executable)
                    if os.path.isfile(f):
                        return p

        ####################################################
        # apply to JEM-enabled subjobs

        self.__monitoredSubjobs = []
        try:
            for i, config in enumerate(subjobconfig):
                if (i == 0) or (i % jemconfig['JEM_MONITOR_SUBJOBS_FREQ'] == 0):
                    logger.debug("Enabling JEM realtime monitoring for job #" + self.__getFullJobId() + "." + str(i))

                    self.__monitoredSubjobs += [i]

                    # as we're replacing the executable with our wrapper script, the executable probably
                    # has to be seperately put into the input sandbox - let's check if it is located in
                    # /bin, /usr/bin, /sbin or /usr/sbin first, because in that cases, we don't have to
                    # provide it ourselves...
                    addToBox = True
                    sbPath = "."
                    if isinstance(config.exe, File):
                        sbPath = config.exe.subdir
                        config.exe = config.exe.name
                    p = getPath(config.exe)
                    if p:
                        if (p[:4] == os.sep + "bin") or (p[:5] == os.sep + "sbin") \
                        or (p[:8] == os.sep + "usr" + os.sep + "bin") \
                        or (p[:9] == os.sep + "usr" + os.sep + "sbin"):
                            addToBox = False

                    if addToBox:
                        if type(config.exe) == type(""):
                            config.exe = File(config.exe, sbPath)
                        config.inputbox += [config.exe]

                    # now add JEMs files to the boxes...
                    config.inputbox += jemInputBox
                    config.outputbox += jemOutputBox

                    # set the executable to our main WN script, and the original exe to its 1st argument
                    theArgs = [config.getExeString()] + config.args

                    # if we're running at Localhost, inject virtual jobID (d'oh...)
                    if self.__job.backend.__class__.__name__ == "Localhost":
                        theArgs = ["--jobid", self.__job.info.monitor.getJobID()] + theArgs

                    config.args = theArgs
                    config.exe = submitExecutable

                    # set additional environment variables needed by JEM
                    # determine if we are an Athena job...
                    jobIsAthena = self.__isAthenaJob()

                    # change job environment
                    if self.__job.info.monitor.ctracer.enabled:
                        if jobIsAthena:  # The run application for Athena jobs always is Python!
                            config.env['JEM_CTRACE_APPS'] = "__find_python__"
                        else:
                            config.env['JEM_CTRACE_APPS'] = self.__job.info.monitor.ctracer.traceApps

                        config.env['JEM_CTRACE_MODULES'] = self.__job.info.monitor.ctracer.traceModules

                        if config.env['JEM_CTRACE_APPS'] == '' and config.env['JEM_CTRACE_MODULES'] != '':
                            config.env['JEM_CTRACE_APPS'] = config.env['JEM_CTRACE_MODULES']
                    else:
                        config.env['JEM_CTRACE_DISABLE'] = "1"

                    config.env["JEM_UI_USER"] = str(os.getuid())

                    if self.__httpsListenPort != 0:
                        config.env["JEM_UI_LISTEN_PORT"] = str(self.__httpsListenPort)

                    # commit all changes we did to the subjobconfig
                    try:
                        config.processValues()
                    except:
                        import traceback
                        ei = sys.exc_info()
                        logger.error("  error occured while preparing: " + str(ei[0]) + ": " + str(ei[1]))
                        logger.debug("  trace:\n" + "".join(traceback.format_tb(ei[2])))

                        # this may fail if the executable is no user script, but just some
                        # usually available command like 'echo'. So, just ignore the failure
                        # (if the config.exe *is* a user app, and still the addition fails,
                        #  the job won't run at all - this should be easily debug-able...)
                        logger.debug("config object:\n" + str(config))

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
        # no action for subjobs...
        if self.__job.master:
            return

        if self.__job.subjobs and len(self.__job.subjobs) > 0:
            # HACK: If we are a split job, we have to wait for the subjobs to be assigned
            #       their backend-id (JobID) before we can start the Job-Listener process
            from Ganga.Core.GangaThread import GangaThread
            class WaitForJobIDsThread(GangaThread):
                def __init__(self, job, callback):
                    GangaThread.__init__(self, name='wait_for_jobID_thread')
                    self.__job = job
                    self.__callback = callback
                    self.setDaemon(True)
        
                def run(self):
                    logger.debug("started thread waiting for subjob-ids of job #" + str(self.__job.id))
                    while not self.should_stop():
                        if self.__job.subjobs[0].backend.id != "":
                            # yay!
                            logger.debug("Job " + str(self.__job.id) + " has been submitted.")
                            self.__callback()
                            break
                        else:
                            time.sleep(0.25)
                    logger.debug("thread waiting for subjob-ids of job #" + str(self.__job.id) + " exits")
                    self.unregister()
            
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
        self.__userAppRunning = True
        logger.info("Begun to receive monitoring data for job " + str(self.__job.id))


    def complete(self, cause):
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
                    if JEMloader.fpEnabled:
                        logger.info("Copying the full log into the workspace...")
    
                        if os.path.exists(self.__job.info.monitor.jmdfile):
                            try:
                                os.system('mv ' + self.__job.info.monitor.jmdfile + ' ' + self.__job.info.monitor.jmdfile + '.bak')
                                try:
                                    os.system('cp ' + self.__job.outputdir + "JEM_MON.jmd " + self.__job.info.monitor.jmdfile)
                                    logger.debug("  OK, log of main job #" + str(self.__job.id) + " has been copied.")
                                except:
                                    # revert...
                                    os.system('mv ' + self.__job.info.monitor.jmdfile + '.bak ' + self.__job.info.monitor.jmdfile)
                            except:
                                pass

                        logger.debug("  (now trying to copy the subjobs' logs)")
                        
                        for sj in self.__job.subjobs:
                            if os.path.exists(sj.outputdir + "JEM_MON.jmd"):
                                try:
                                    os.system('cp ' + sj.outputdir + "JEM_MON.jmd " + self.__job.info.monitor.jmdfile + ".subjob." + str(sj.id))
                                    logger.debug("  OK, log of subjob #" + str(self.__job.id) + "." + str(sj.id) + " has been copied.")
                                except:
                                    pass
    
                        logger.info("...done. Inspecting the data now should display the whole available info.")


    def rollback(self):
        logger.debug("Job " + self.__getFullJobId() + " got rolled back to new.")
        self.__stopJobListener()


    ################################################################################################

    def __readJEMrc(self, path):
        """
        Read JEM settings from a .JEMrc file into a dictionary.
        """
        theJEMrcSettings = {}
        theSection = ""
        #logger.debug("opening " + path + " for reading.")
        fd = open(path, "r")
        for line in fd:
            if len(line) == 0:
                continue
            m = re.search('^\[(.*)\]', line)        # this is a new section!
            if m != None:
                theSection = m.group(0).strip(" []\t")
                #logger.debug("  found section: [" + theSection + "]")
                if not theJEMrcSettings.has_key(theSection):
                    theJEMrcSettings[theSection] = {}
            else:
                try:
                    k,v = line.strip().split("=", 1)
                    k = k.strip()
                    v = v.strip()
                    if k[0] == "#":        # ignore commented-out settings...
                        continue
                    if theJEMrcSettings[theSection].has_key(k):
                        logger.warning("Duplicate JEM setting '" + k + "' in " + path)
                    #logger.debug("    found: '" + k + "' = '" + v + "'")
                    theJEMrcSettings[theSection][k] = v
                except:
                    #logger.debug("    __readJEMrc catched " + str(sys.exc_info()[0]) + " (" + str(sys.exc_info()[1]) + ")")
                    pass
        #logger.debug("read JEM configuration:\n" + pprint.pformat(theJEMrcSettings))
        return theJEMrcSettings


    def __buildValvesDict(self, JEMrc):
        valves = []
        if JEMrc['JEMConfig'].has_key('PUBLISHER_USE_TYPE'):
            valvesStr = JEMrc['JEMConfig']['PUBLISHER_USE_TYPE']
            try:
                for v in valvesStr.split("|"):
                    v = v.strip()
                    try:
                        v = v.split("_")[-1]
                    except:
                        pass
                    valves += [v]
            except:
                logger.debug("Failed to parse JEM-set valves: " + str(sys.exc_info()[0]) + ": " + str(sys.exc_info()[1]))
                valves = []
        else:
            try:
                valvesVal = WNConfig.PUBLISHER_USE_TYPE
                if valvesVal & PUBLISHER_USE_DEVNULL:
                    valves += ["DEVNULL"]
                if valvesVal & PUBLISHER_USE_RGMA:
                    valves += ["RGMA"]
                if valvesVal & PUBLISHER_USE_MONALISA:
                    valves += ["MONALISA"]
                if valvesVal & PUBLISHER_USE_TCP:
                    valves += ["TCP"]
                if valvesVal & PUBLISHER_USE_FS:
                    valves += ["FS"]
                if valvesVal & PUBLISHER_USE_HTTPS:
                    valves += ["HTTPS"]
                if valvesVal & PUBLISHER_USE_FSHYBRID:
                    valves += ["FSHYBRID"]
            except:
                pass
        return valves


    def __modifyJEMrcSettings(self, JEMrc):
        """
        Modify JEM settings passed in a dictionary according to the Ganga JEM configuration.
        """
        monitor = self.__job.info.monitor

        if not JEMrc.has_key('JEMConfig'):
            JEMrc['JEMConfig'] = {}
        if not JEMrc.has_key('JEMuiConfig'):
            JEMrc['JEMuiConfig'] = {}

        ### loglevels
        JEMrc['JEMConfig']['WRAPPER_BASH_PUBLISHER_LEVEL'] = monitor.advanced.bash_loglevel
        JEMrc['JEMConfig']['WRAPPER_PYTHON_PUBLISHER_LEVEL'] = monitor.advanced.python_loglevel

        ### valves
        valves = self.__buildValvesDict(JEMrc)

        gangaConfiguredValves = []
        try:
            gangaConfiguredValves = self.getJobObject().backend.monitoring.advanced.valves
        except:
            pass

        if gangaConfiguredValves != []:
            valves = gangaConfiguredValves

        if jemconfig['JEM_ENABLE_REALTIME'] == False: # disable valves if no realtime transfer is wanted
            for v in ("HTTPS", "FSHYBRID", "TCP", "RGMA", "MONALISA"):
                if v in valves: valves.remove(v)

        s = ""
        for v in valves:
            if s != "":
                s += " | "
            s += "PUBLISER_USE_" + v

        JEMrc['JEMConfig']['PUBLISHER_USE_TYPE'] = s

        #logger.debug("effective JEM configuration now:\n" + pprint.pformat(JEMrc))

        ### done
        #jemconfig['JEM_LOGFILE_MAXSIZE']


    def __writeJEMrc(self, theJEMrcSettings):
        """
        Write JEM settings from a dictionary to a .JEMrc file (in the temp-directory). Return its path.
        """
        thePath = os.sep + "tmp" + os.sep + "JEMtmp"  
        if not os.path.exists(thePath):
            os.mkdir(thePath)
            
        thePath += os.sep + str(os.getuid())
        if not os.path.exists(thePath):
            os.mkdir(thePath)
            
        thePath += os.sep + ".JEMrc"
        fd = open(thePath, "w")

        for section,settings in theJEMrcSettings.items():
            fd.write("[" + section + "]\n")
            for k,v in settings.items():
                fd.write(k + "=" + str(v) + "\n")
        fd.close()

        logger.debug("Wrote modified user JEM settings to " + thePath)

        return thePath


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


    def __tryFindFreeListeningPort(self, retries = 20):
        """
        Find a free port by trying to connect to ports 50000..50000+retries
        """

        if not ((WNConfig.PUBLISHER_USE_TYPE & WNConfig.PUBLISHER_USE_HTTPS) \
        or (WNConfig.PUBLISHER_USE_TYPE & WNConfig.PUBLISHER_USE_FSHYBRID)):
            return 0

        try:
            port = int(WNConfig.PUBLISHER_HTTPS_SERVER.split(":")[1])
            if port != 0:
                logger.debug('Using configured port: ' + port)
                return port
        except:
            pass

        if JEMMonitoringServiceHandler._freeportfinder:
            logger.debug("Attempting to find a free listening port...")
            try:
                port = JEMMonitoringServiceHandler._freeportfinder.tryFindFreeListeningPort(retries)
                if port:
                    logger.debug('Found free port: %d' % port)
                    return port
            except:
                pass

        logger.warn('Cannot find a free port for the JEM listener process.')
        return 0


    def __startJobListener(self):
        """
        Start the job listener for live monitoring support
        """
        if not isinstance(self.__job.info.monitor, JobExecutionMonitor.JobExecutionMonitor):
            return

        if not self.__job.info.monitor.realtime or not jemconfig['JEM_ENABLE_REALTIME']: # pylint: disable-msg=E1101
            return

        try:
            jobID = self.__job.info.monitor.getJobID()
            escapedJobID = Utils.escapeJobID(jobID)
            
            # debug output ##############################################################
            logger.debug('Trying to launch JEM realtime monitoring listener process')   #
                                                                                        #
            s = '  for job with id "%s"' % jobID                                       #
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
            executable = JEMloader.JEM_PACKAGEPATH + os.sep + 'JEMganga' + os.sep + 'LiveMonitoring.py'
            args = [executable]
            if self.__httpsListenPort != 0:
                args += ["--https-port", str(self.__httpsListenPort)]
                self.__job.info.monitor.port = self.__httpsListenPort
            args += [jobID]
            
            # further job IDs (subjob IDs)
            for i, sj in enumerate(self.__job.subjobs):
                if i in self.__monitoredSubjobs:
                    args += [str(sj.backend.id)]

            try:
                self.__job.info.monitor.pid = os.spawnve(os.P_NOWAIT, executable, args, os.environ)
                self.__job.info.monitor.jmdfile = WNConfig.LOG_DIR + os.sep + escapedJobID + os.sep + UIConfig.PUBLISHER_JMD_FILE
            except Exception, r:
                logger.error('Could not start job listener process.')
                logger.error('The Job with the id %s will start without monitoring.' % jobID)
                logger.error('Error cause: %s.' % str(r))
                if self.pid > -1:
                    self.__stopJobListener()
                self.__job.info.monitor.enabled = False # pylint: disable-msg=E1101
                return

            logger.info('JEM realtime monitoring listener started.')
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
        Kill the job listener of this job
        FIXME
        """
        if not isinstance(self.__job.info.monitor, JobExecutionMonitor.JobExecutionMonitor):
            return
        
        self.__job.info.monitor.abortWatch()

        if not self.__job.info.monitor.realtime or not jemconfig['JEM_ENABLE_REALTIME']: # pylint: disable-msg=E1101
            return

        if self.__job.info.monitor.pid != -1:
            theServer = self.__checkServer()
            for pid in theServer:
                self.__killprocess(pid)
            self.__killprocess(self.__job.info.monitor.pid)


    def __checkServer(self):
        """
        Get the PIDs of the server-processes for this job
        """
        # not the finest solution, but it works :/
        if JEMloader.rgmaPubEnabled:
            servername = JEMConfig.SERVER_RGMA_EXE
        elif JEMloader.httpsPubEnabled:
            if JEMloader.httpsExternal:
                return []
            servername = JEMConfig.SERVER_HTTPS_EXE
        elif JEMloader.tcpPubEnabled:
            servername = JEMConfig.SERVER_TCP_EXE

        serverpids = []
        pids,ppids,cmds = self.__getChildProcesses()
        for z,p in enumerate(pids):
            if ppids[z] == str(self.__job.info.monitor.pid) and cmds[z].find(servername) != -1:
                serverpids += [p]
        return serverpids


    def __getChildProcesses(self):
        """
        Find the child-processes of LiveMonitoring.py (Server, PipePublisher-launched stuff, etc).
        """

        # prepare for grep
        jobID = self.__job.info.monitor.getJobID() # pylint: disable-msg=E1101
        logDir = JEMConfig.MON_LOG_DIR + os.sep + jobID

        # dont delete the width parameter, otherwise the grep command will fail due to line length...
        cmd = "ps --width 1000 -eo user,pid,ppid,command | grep " + getpass.getuser() + " | awk '{ print $1, $2, $3, $4,$5,$6,$7,$8,$9 }'"

        # save results to tmp file
        if not os.path.exists(logDir):
            os.makedirs(logDir)
        tmpFile = logDir + os.sep + "tmpServerActiveCheck"
        os.system(cmd + " > " + tmpFile)
        fd = open(tmpFile,'r')

        pids = []
        ppids = []
        cmds = []

        # read result
        tmpString = fd.readline()
        while tmpString:
            user,pid,ppid,cmd = tmpString.split(" ", 3) # pylint: disable-msg=W0612
            pids.append(pid)
            ppids.append(ppid)
            cmds.append(cmd)
            tmpString = fd.readline()
        os.remove(tmpFile)
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

        try:
            # try to terminate gracefully
            os.kill(pid, signal.SIGTERM)

            # wait for the process to finish
            time.sleep(2)

            # if the process is still there, KILL it :)
            # (workaround for python bug, waitpid always raises oserror, so it can not be used here)

            # first, look if we got childprocesses to kill first:
            pids,ppids,cmds = self.__getChildProcesses() # pylint: disable-msg=W0612
            for z,p in enumerate(pids):
                if ppids[z] == pid:
                    try:
                        os.kill(p, signal.SIGKILL)
                    except OSError, ose:
                        if str(ose).find("No such process"):
                            return
                        else:
                            os.system("kill -9 " + str(p) + " 2> /dev/null")

            try:
                os.kill(pid, signal.SIGKILL)
            except OSError, ose:
                if str(ose).find("No such process"):
                    return
                else:
                    os.system("kill -9 " + str(pid) + " 2> /dev/null")
        except:
            # try to kill process with os call, will always work (or not, if the process doesn't exist...)!
            try:
                os.system("kill -9 " + str(pid) + " 2> /dev/null")
            except:
                return
