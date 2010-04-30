"""
This is the main GPI module of the Ganga integration of the JobExecutionMonitor (JEM).
It defines the GangaObject-derivate JEM uses as an interface for the Ganga-user. She can
access JEMs methods by j.info.monitor.<method>.

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
from Ganga.GPIDev.Schema import * # pylint: disable-msg=W0401
from Ganga.GPIDev.Base import GangaObject

from Ganga.GPIDev.Lib.File import File
from Ganga.Utility.logging import getLogger, logging
from Ganga.Utility.Config import makeConfig, getConfig, ConfigError
from Ganga.Core.GangaThread import GangaThread


########################################################################################################################
# Our logging instance.
logger = getLogger("GangaJEM.Lib.JEM")
outlogger = getLogger("GangaJEM.Lib.JEM.out")

########################################################################################################################
# JEM global configuration options.
jemconfig = makeConfig('JEM', 'Configuration parameters for the Job Execution Monitor')

jemconfig.addOption('JEM_ENABLE', False,
                    "Set this to 'False' to globally disable the JEM monitoring, overriding the monitoring-settings of Job objects.")
jemconfig.addOption('JEM_ENABLE_REALTIME', True,
                    "Set this to 'False' to globally disable realtime monitoring. Monitoring data will only be available in each job's output sandbox.")
jemconfig.addOption('JEM_ENABLE_CTRACE', False,
                    "Set this to 'False' to globally disable c/c++ module tracing.")
jemconfig.addOption('JEM_REPACK', False,
                    'Wether to repack the JEM library before each job submission. This is useful mostly for developers.')
jemconfig.addOption('JEM_MONITOR_SUBJOBS_FREQ', 10000,
                    'Enable JEM monitoring only for every N-th subjob of a splitjob.')


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
        # LEGACY MODULES
        import JEMlib
        from JEMlib.conf import JEMSysConfig as SysConfig
        from JEMui.conf import JEMuiSysConfig as JEMConfig
        from JEMlib.conf import JEMConfig as WNConfig
        from JEMui.conf import JEMuiConfig as UIConfig
        from JEMlib.utils.ReverseFileReader import ropen
        from JEMlib.utils.DictPacker import multiple_replace
        from JEMlib.utils import Utils
        from JEMlib.utils import uuid

        from Common.Config import Config as JEM3Config
    except:
        logger.debug("Something went wrong when importing JEMs core modules:")
        logger.debug(str(sys.exc_info()[0]) + ": " + str(sys.exc_info()[1]))
        JEMloader.INITIALIZED = False

    try:
        # this is a HACK to pull all config options off of JEM and create GangaObject-representations of them.
        from Modes.Ganga import ConfigConverter
        definition, objlist = ConfigConverter.JEMConfig2GangaObjectSchemas()
        exec(definition, globals(), locals())
    except:
        logger.debug("Failed to inject JEMs config into GangaJEM")
        ei = sys.exc_info()
        logger.debug("Reason: " + str(ei[0]) + " - " + str(ei[1]))


#####################################################################################################################################################
#####################################################################################################################################################
class JEMCTraceOptions(GangaObject):
    """JEM - The Job Execution Monitor - C/C++-Tracer configuration.

    This object represents the configuration of the C/C++ module tracing subsystem of JEM.

    The ctracer must be seperately enabled; set 'enabled' to True to do this. Also, to use
    the ctracer, the to-be-traced module(s) must be specified.

    The to-be-traced module(s) must be defined in 'traceModules', whereas the application(s)
    that load the to-be-traced module(s) must be defined in 'traceApps'. If an executable
    should be traced, usually the contents of 'traceModules' and 'traceApps' is equal. The
    values of 'traceModules' and 'traceApps' usually differ if a shared library should be
    traced. If 'traceApps' is left empty, it is assumed to be equal to 'traceModules'.

    For both 'traceModules' and 'traceApps', several entries can be made, seperated by
    whitespace, commas or colons. Note that either the relative path from the working
    directory on the grid worker node, or an absolute path must be specified for the trace-
    module(s) and -app(s).

    Examples:

    traceModules               traceApps                  behaviour
    -------------------------------------------------------------------------------------------
    '/usr/bin/echo'            '/usr/bin/echo'            events in /usr/bin/echo are traced.
    './myApp'                  './myApp'                  events in ./myApp are traced.
    './myLib.so'               './myApp'                  events in ./myLib are traced IF it
                                                          is loaded by ./myApp (in other words,
                                                          if the RUN executable is myApp, with
                                                          myLib dynamically linked to it)
    './libA.so,./libB.so'      '/usr/bin/python'          events in the two user libraries are
                                                          traced for python scripts loading
                                                          and using them.
    -------------------------------------------------------------------------------------------

    Note for ATLAS-related jobs (Athena users):
    If the job's application is an Athena-instance, the user algorithm's library has to be
    specified in 'traceModules', but 'traceApps' can be left blank (it is set automatically by
    JEM to the python interpreter on the worker node running athena.py).

    Example setting for 'traceModules':
    './work/Control/AthenaExamples/AthExHelloWorld/i686-slc4-gcc34-opt/libAthExHelloWorld.so'


    The resolving of symbol values allows you to inspect the user algorithm's memory at each
    logged step, much like a remote debugger provides. Please be aware that this slows down
    the application's execution by a large amount. Also, the maximum nesting depth at which
    struct members are resolved and pointers are followed has a large impact on application
    performance. You might consider disabling the ctracer for normal job runs and resubmit
    failing jobs with increasing verbosity (maxStructDepth settings, resolveValues setting)
    until the issue is resolved.

    See also: http://www.grid.uni-wuppertal.de/grid/jem

    JEM (c)2004-2009 Bergische Universitaet Wuppertal

    """
    _schema = Schema(Version(0,1), {
        'enabled'               : SimpleItem(defvalue=False,\
                                             doc='Enables C/C++ module tracing'),
        'traceModules'          : SimpleItem(defvalue='',\
                                             doc='The module(s) to be traced'),
        'traceApps'             : SimpleItem(defvalue='',\
                                             doc='The application loading/being the to-be-traced module(s)'),
        'resolveValues'         : SimpleItem(defvalue=False,\
                                             doc='Enable resolving of symbol values in the ctracer'),
        'maxStructDepth'        : SimpleItem(defvalue=2,\
                                             doc='Max nesting depth to resolve struct members and pointers'),
    })

    _category = 'JEMCTraceOptions'
    _name = 'JEMCTraceOptions'


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

    Since version alpha 0.2.3, JEM includes a C/C++ module tracing subsystem - the ctracer. it is
    configured in an own config subobject, 'ctracer'. The ctracer-config can be accessed with
    "j.info.monitor.ctracer". Type 'help(JEMCTraceOptions)' for more information on setting up and
    using the ctracer. NOTE: for version 0.3.0 onwards, this feature is not available yet.

    Note that when using splitjobs, by default JEM is enabled only for every 100th subjob to
    prevent monitoring data flooding. You can change this behaviour in .gangarc.

    Methods of interest for the everyday-usage of JEM are:

    getStatus()                  prints status information about the monitored job
    getMetrics()                 prints current WN metrics (load, RAM usage, disk space, etc)
    plotMetrics()                plots the recorded WN metrics using gnuplot (must be installed)
    listExceptions()             lists the last some exceptions that happened
    listCommands()               lists the last some commands / calls / returns that happened
    showException()              prints verbose information about an exception
    showCommand()                prints verbose information about a command / call / return
    peek()                       peeks into the job's stdout/-err streams (like 'tail')
    livePeek()                   prints the job's stdout as it is created (like 'tail -f')
    extractLogfiles()            extracts JEMs logfiles (available after the job finished)
    waitForRealStart()           waits until the the user application on the WN has started
                                 (this wait may be aborted by pressing <return>)

    You can get further help on each of those methods by typing e.g.

        'help(JobExecutionMonitor.getStatus)'.


    Furthermore, you can get an overview of running JEM listener instances by typing

        'JEMlisteners()'



    NOTE: This is an ALPHA version of JEM. If you have any comments, suggestions or encounter a
          bug, please don't hesitate to give feedback! Visit our webpage for contact information.

    See also: http://www.grid.uni-wuppertal.de/grid/jem

    JEM (c)2004-2009 Bergische Universitaet Wuppertal

    """
    _schema = Schema(Version(0,2), {
        'anonymous'              : SimpleItem(hidden=True, defvalue=False, doc='internal anonymize flag'),
        'enabled'                : SimpleItem(defvalue=True, doc='Enables JEM monitoring for the job'),
        'realtime'               : SimpleItem(defvalue=False, doc='Enables realtime reception of monitoring data'),
        'ctracer'                : ComponentItem('JEMCTraceOptions', summary_print='_summary_print',\
                                                 doc='Configuration of the C/C++ module tracing subsystem'),
        'advanced'               : ComponentItem('JEMAdvancedOptions', summary_print='_summary_print',\
                                                 doc='Advanced configuration'),
        'jobID'                  : SimpleItem(hidden=True, defvalue=None, protected=1, copyable=0, transient=1,\
                                              typelist=['type(None)', 'type(str)'],\
                                              doc='Real backend-jobID (or generated)'),
        'pid'                    : SimpleItem(hidden=True, defvalue=0, protected=1, copyable=0,\
                                              doc='Process id of the job listener'),
        'port'                   : SimpleItem(hidden=True, defvalue=0, protected=1, copyable=0,\
                                              doc='Port number of the job listeners HTTPS server'),
        'jmdfile'                : SimpleItem(hidden=True, defvalue='', protected=1, copyable=0,\
                                              doc='Path to jmd logfile'),
        'userAppRunning'         : SimpleItem(hidden=True, defvalue=False, protected=1, copyable=0,\
                                              doc='Has the user application on the WN started running yet?'),
        'userAppExited'          : SimpleItem(hidden=True, defvalue=False, protected=1, copyable=0,\
                                              doc='Has the user application on the WN finished yet?'),
        'watcherThread'          : SimpleItem(hidden=True, defvalue=None, protected=1, copyable=0, transient=1,\
                                              typelist=['type(None)', 'JEMWatcherThread'], load_default=0, optional=1,\
                                              doc='Internal watcher thread handle')
    })

    _category = 'monitor'           # allow insertion into Job.JobInfo object
    _name = 'JobExecutionMonitor'   # GPI-public classname

    # methods accessible from GPI
    _exportmethods = ['getStatus', 'getMetrics', 'plotMetrics', 'listExceptions', 'listCommands',\
                      'showException', 'showCommand', 'peek', 'getListenerLog', 'waitForRealStart',\
                      'livePeek', 'watch', 'abortWatch', 'extractLogfiles',\
                      '_getListenerPid', '_getServerPid', '_getServerPort', '_getServerStatus',\
                      '_hasUserAppStarted', '_hasUserAppExited', '_getTransmissionStats']

    def __init__(self):
        GangaObject.__init__(self)

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
                fd.write("yes\n")
                fd.close()
                self.anonymous = True
                print "*   ...no. statistics about your job runs will be anonymized."
            except:
                pass
        else:
            if os.path.exists(p + "/anonymous"):
                self.anonymous = True

    ####################################################################################################################
    ### public interface (methods exported via _exportmethods)

    def getStatus(self):
        """
        This method prints basic information about the running job, like its job-id
        and the worker node the job is running on.
        """
        if self.__checkStatus():
            s = PrettyStrings.makeHeader("current job status")
            job = self.getJobObject()

            appstate = "In progress"
            l = self.__seekJMDinfo("JOBSTATE", 1, 1)
            if len(l) and l[0]:
                if l[0].has_key("Status"):
                    if l[0]["Status"] == "FINISHED":
                        appstate = "Finished"
                        self.userAppExited = True

            # look for the last RESOURCE message; if found, extract the WN-name
            l = self.__seekJMDinfo("RESOURCE", 1, 1)
            if len(l) and l[0]:
                l = l[0]
                s += PrettyStrings.formatDatum("status (middleware)", str(job.backend.status)) # pylint: disable-msg=E1101
                s += PrettyStrings.formatDatum("status (user application)", appstate)
                s += PrettyStrings.formatDatum("job-id", str(job.backend.id)) # pylint: disable-msg=E1101
                s += PrettyStrings.formatDatum("compute element", str(job.backend.actualCE)) # pylint: disable-msg=E1101

                if l.has_key("WN"):
                    s += PrettyStrings.formatDatum("worker node", l["WN"])

                l = self.__seekJMDinfo("EXCEPTION", 6, 1)
                es = "none"
                if len(l) > 0:
                    if len(l) < 6:
                        es = str(len(l))
                    else:
                        es = "more than 5"
                s += PrettyStrings.formatDatum("exceptions logged", es)

                outlogger.info(s)
            else:
                outlogger.info("no status information received yet")


    def getMetrics(self):
        """
        This method prints information about system the job runs on, like the CPU usage,
        network traffic, and free disk space
        """
        if self.__checkStatus():
            # look for the last RESOURCE message; if found, extract metrics
            l = self.__seekJMDinfo("RESOURCE", 1, 1)
            if len(l) and l[0]:
                l = l[0]

                s = PrettyStrings.makeHeader("worker node system metrics (measured " + PrettyStrings.formatTime(l) + ")")

                if l.has_key("Load"):
                    s += PrettyStrings.formatDatum("load", l["Load"])
                if l.has_key("Mem"):
                    s += PrettyStrings.formatDatum("memory ", l["Mem"])

                s += "network traffic\n"
                if l.has_key("NetR"):
                    s += PrettyStrings.formatDatum("    inbound", l["NetR"])
                if l.has_key("NetT"):
                    s += PrettyStrings.formatDatum("    outbound", l["NetT"])

                s += "available disk space in\n"
                if l.has_key("Workdir"):
                    s += PrettyStrings.formatDatum("    working directory", l["Workdir"])
                if l.has_key("Tmp"):
                    s += PrettyStrings.formatDatum("    tmp", l["Tmp"])
                if l.has_key("Home"):
                    s += PrettyStrings.formatDatum("    home directory", l["Home"])
                if l.has_key("Swap"):
                    s += PrettyStrings.formatDatum("    swap", l["Swap"])

                if l.has_key("FullSys") and len(l["FullSys"]) > 0:
                    s += PrettyStrings.formatDatum("full filesystems", l["FullSys"])

                outlogger.info(s)
            else:
                outlogger.info("no status information received yet")


    def listExceptions(self, n = 5, start = 1, ascending = False):
        """
        This method lists exceptions happened during the job run. For details about
        an exception, see showException().

        @param n: how many exceptions to list. default = 5
        @param start: the list begins at the start-th exception. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        """
        if self.__checkStatus():
            if ascending:
                orderString = "first "
            else:
                orderString = "last "
            s = ""
            l = self.__seekJMDinfo("EXCEPTION", n, start, ascending)
            z = start
            for data in l:
                ss = "(" + str(z).ljust(5) + ") " + PrettyStrings.formatTime(data) + " : "

                if data.has_key("Error"):
                    ss += data["Error"][:48]
                else:
                    ss += "unknown error"

                ss += " in " + PrettyStrings.formatLocation(data) + "\n"
                if ascending:
                    s += ss
                else:
                    s = ss + s
                z += 1

            s = PrettyStrings.makeHeader("listing exceptions (" + orderString + str(n) + ", skipping " + str(start - 1) + ")") + s
            outlogger.info(s)


    def listCommands(self, n = 5, start = 1, ascending = False):
        """
        This method lists commands executed during the job run. For details about
        a command, see showCommand(). Commands can be commands executed in a script,
        function calls or function returns.

        @param n: how many commands to list. default = 5
        @param start: the list begins at the start-th command. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        """
        if self.__checkStatus():
            if ascending:
                orderString = "first "
            else:
                orderString = "last "
            s = ""
            l = self.__seekJMDinfo("COMMAND", n, start, ascending)
            z = start
            for data in l:
                ss = "(" + str(z).ljust(5) + ") " + PrettyStrings.formatTime(data) + " : "
                if data.has_key("SubType"):
                    if data["SubType"] == "CALL":
                        ss += "call to ".ljust(16)
                        ss += PrettyStrings.formatLocation(data)
                    elif data["SubType"] == "RETURN":
                        ss += "return from ".ljust(16)
                        ss += PrettyStrings.formatLocation(data)
                    elif data["SubType"] == "BUILTIN":
                        ss += "built-in cmd: ".ljust(16)
                        if data.has_key("M1"):
                            ss += PrettyStrings.formatString(data["M1"], 64)
                        else:
                            ss += "<unknown>"
                    elif data["SubType"] == "EXTERNAL":
                        ss += "command: ".ljust(16)
                        if data.has_key("M1"):
                            ss += PrettyStrings.formatString(data["M1"], 64)
                        else:
                            ss += "<unknown>"
                    elif data["SubType"] == "SYNTAX":
                        ss += "script expr: ".ljust(16)
                        if data.has_key("M1"):
                            ss += PrettyStrings.formatString(data["M1"], 64)
                        else:
                            ss += "<unknown>"
                    else:
                        ss += "misc event"
                else:
                    ss += "misc event"
                ss += "\n"
                if ascending:
                    s += ss
                else:
                    s = ss + s
                z += 1
            s = PrettyStrings.makeHeader("listing commands (" + orderString + str(n) + ", skipping " + str(start - 1) + ")") + s
            outlogger.info(s)


    def showException(self, n = 1, ascending = False):
        """
        This method prints detailled information about an exception that
        happened during the job run.

        @param n: The n-th exception is shown. default = 1
        @param ascending: Wether n counts from the beginning of the list. default = False
        """
        if self.__checkStatus():
            l = self.__seekJMDinfo("EXCEPTION", 1, n, ascending)
            if len(l):
                data = l[0]
                s = PrettyStrings.makeHeader("exception info")
                s += PrettyStrings.formatDatum("time", PrettyStrings.formatTime(data))

                if data.has_key("Error"):
                    s += PrettyStrings.formatDatum("type", data["Error"])
                else:
                    s += PrettyStrings.formatDatum("type", "unknown error")

                if data.has_key("Reason"):
                    s += PrettyStrings.formatDatum("reason / value", data["Reason"])

                if data.has_key("Frame"):
                    if data["Frame"] == "?" and data.has_key("Lang") and data["Lang"] == "PYTHON":
                        s += PrettyStrings.formatDatum("frame", "<module>")
                    else:
                        s += PrettyStrings.formatDatum("frame", PrettyStrings.formatString(data["Frame"], 128))
                else:
                    s += PrettyStrings.formatDatum("frame", "<unknown>")

                if data.has_key("Script"):
                    s += PrettyStrings.formatDatum("file", data["Script"])
                else:
                    s += PrettyStrings.formatDatum("file", "<unknown>")

                if data.has_key("Line"):
                    s += PrettyStrings.formatDatum("line", data["Line"])
                else:
                    s += PrettyStrings.formatDatum("line", "<unknown>")

                if data.has_key("TB") or data.has_key("Code") or data.has_key("Vars"):
                    s += "\n"

                if data.has_key("TB"):
                    s += "backtrace:\n"
                    for ss in data["TB"].split("<br>"):
                        s += PrettyStrings.formatString(ss, 128) + "\n"

                if data.has_key("Code"):
                    s += "code vicinity:\n"
                    for ss in data["Code"].split("<br>"):
                        s += PrettyStrings.formatString(ss, 128) + "\n"

                if data.has_key("Vars"):
                    s += "scope variables:\n"
                    for ss in data["Vars"].split("<br>"):
                        try:
                            vname, value = ss.split(": ")
                            s += PrettyStrings.formatString(vname, 24) + " : " + PrettyStrings.formatString(value, 96) + "\n"
                        except:
                            pass

                outlogger.info(s)
            else:
                outlogger.warn("No such exception")


    def showCommand(self, n = 1, ascending = False):
        """
        This method prints detailled information about a command that
        happened during the job run.

        @param n: The n-th command is shown. default = 1
        @param ascending: Wether n counts from the beginning of the list. default = False
        """
        if self.__checkStatus():
            l = self.__seekJMDinfo("COMMAND", 1, n, ascending)
            if len(l):
                data = l[0]
                if not data.has_key("SubType"):
                    outlogger.warn("Unknown type of command")
                    return

                lang = "UNKNOWN"
                if data.has_key("Lang"):
                    lang = data["Lang"]

                ss = PrettyStrings.formatDatum("time", PrettyStrings.formatTime(data))

                if lang != "UNKNOWN":
                    ss += PrettyStrings.formatDatum("language", lang)

                if data.has_key("Frame"):
                    if (data["Frame"] == "?" and lang == "PYTHON") or (data["Frame"] == "???" and lang == "BASH"):
                        ss += PrettyStrings.formatDatum("frame", "<module>")
                    else:
                        ss += PrettyStrings.formatDatum("frame", PrettyStrings.formatString(data["Frame"], 128))
                else:
                    ss += PrettyStrings.formatDatum("frame", "<unknown>")

                if data.has_key("Script"):
                    ss += PrettyStrings.formatDatum("file", data["Script"])
                else:
                    ss += PrettyStrings.formatDatum("file", "<unknown>")

                if data.has_key("Line"):
                    ss += PrettyStrings.formatDatum("line", data["Line"])
                else:
                    ss += PrettyStrings.formatDatum("line", "<unknown>")

                s = ""
                if data["SubType"] == "CALL":
                    s = PrettyStrings.makeHeader("function call info")
                    s += ss

                    if data.has_key("M2"):
                        if data["M2"] == "?" and lang == "PYTHON":
                            s += PrettyStrings.formatDatum("caller", "<module>")
                        else:
                            s += PrettyStrings.formatDatum("caller", PrettyStrings.formatString(data["M2"], 128))
                    else:
                        s += PrettyStrings.formatDatum("caller", "<unknown>")

                    if data.has_key("M3"):
                        s += PrettyStrings.formatDatum("caller file", data["M3"].split(":")[0])
                        value = data["M3"].split(":")
                        if len(value) > 1:
                            s += PrettyStrings.formatDatum("caller line", value[1])
                        else:
                            if data.has_key("M4") and lang == "PYTHON":
                                s += PrettyStrings.formatDatum("caller line", data["M4"])
                            else:
                                s += PrettyStrings.formatDatum("caller line", "<unknown>")
                    else:
                        s += PrettyStrings.formatDatum("caller file", "<unknown>")
                        s += PrettyStrings.formatDatum("caller line", "<unknown>")

                    # local vars
                    ss = None
                    if lang != "PYTHON" and lang != "BASH" and data.has_key("M4"):
                        ss = multiple_replace({"_&1_": "'", "_&2_": "=", "_&3_": ";"}, data["M4"])

                    s += PrettyStrings.formatVarList("scope variables:", ss)

                    # arguments
                    ss = None
                    if lang != "PYTHON" and lang != "BASH" and data.has_key("M1"):
                        ss = multiple_replace({"_&1_": "'", "_&2_": "=", "_&3_": ";"}, data["M1"])
                    if (lang == "PYTHON" or lang == "BASH") and data.has_key("M1"):
                        ss = data["M1"]
                        if ss[0:2] == "{{" and ss[-2:] == "}}":
                            ss = ss[1:-1]

                    s += PrettyStrings.formatVarList("call arguments:", ss)

                elif data["SubType"] == "RETURN":
                    s = PrettyStrings.makeHeader("function return info")
                    s += ss

                    if lang == "PYTHON" and data.has_key("M1"):
                        s += PrettyStrings.formatDatum("return value", data["M1"])

                elif data["SubType"] == "BUILTIN":
                    s = PrettyStrings.makeHeader("builtin command info")
                    s += ss

                    if data.has_key("M1"):
                        s += PrettyStrings.formatDatum("command", multiple_replace({"_&1_": "'", "_&2_": "=", "_&3_": ";"}, data["M1"]))

                elif data["SubType"] == "EXTERNAL":
                    s = PrettyStrings.makeHeader("command info")
                    s += ss

                    if data.has_key("M1"):
                        s += PrettyStrings.formatDatum("command", multiple_replace({"_&1_": "'", "_&2_": "=", "_&3_": ";"}, data["M1"]))

                elif data["SubType"] == "SYNTAX":
                    s = PrettyStrings.makeHeader("script expression info")
                    s += ss

                    if data.has_key("M1"):
                        s += PrettyStrings.formatDatum("expression", multiple_replace({"_&1_": "'", "_&2_": "=", "_&3_": ";"}, data["M1"]))

                else:
                    outlogger.warn("Unknown type of command")
                    return

                outlogger.info(s)
            else:
                outlogger.warn("No such command")


    def peek(self, n = 20, start = 1, ascending = False, mode="stdout"):
        """
        This method peeks into the job's output in almost real-time. Output can
        be stdout or stderr-output. Note: The peek-option of JEM must be acti-
        vated, and peeking is not always possible!

        @param n: how many lines to list. default = 20
        @param start: the list begins at the start-th line. default = 1
        @param ascending: wether to start at the beginning of the list. default = False
        @param mode: Chooses what output channel to peek ("stdout" or "stderr"). default = "stdout"
        """
        if mode != "stderr" and mode != "stdout":
            mode = "stdout"
        if self.__checkStatus():
            if ascending:
                orderString = "first "
            else:
                orderString = "last "

            if mode == "stdout":
                ptype = "OUTPEEKLINE"
            else:
                ptype = "ERRPEEKLINE"

            s = ""

            l = self.__seekJMDinfo(ptype, n, start, ascending)
            z = start
            for data in l:
                ss = "(" + str(z).ljust(5) + ") " + PrettyStrings.formatTime(data) + " | "
                if data.has_key("M1"):
                    ss += PrettyStrings.formatString(multiple_replace({"_&1_": "'", "_&2_": "=", "_&3_": ";"}, data["M1"]), 128)
                ss += "\n"

                if ascending:
                    s += ss
                else:
                    s = ss + s

                z += 1

            s = PrettyStrings.makeHeader("peeking at output (" + orderString + str(n) + " of " + mode + ", skipping " + str(start - 1) + ")") + s
            outlogger.info(s)
        else:
            outlogger.info("no status information received yet")


    def livePeek(self):
        def keyPressed():
            return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])

        print "Live peeking into the job's stdout/stderr. Press any key (and then wait for up to 5 seconds) to abort."
        if not self.__checkStatus():
            print "Note: No status information was received yet. Waiting for the user app to start..."
            try:
                time.sleep(5)
                while not self.__checkStatus():
                    if keyPressed():
                        logger.info("Aborted live-peek.")
                        return
                    time.sleep(5)
            except:
                pass

        if not self.userAppRunning:
            while not self.userAppRunning:
                if keyPressed():
                    logger.info("Aborted live-peek.")
                    return
                time.sleep(0.5)

        print "Live peek starts!"
        history = []
        while not keyPressed():
            l = self.__seekJMDinfo("OUTPEEKLINE", 40, 0, False)
            s = ""
            for data in l:
                if not data.has_key("ExactTS") or not data.has_key("M1"):
                    continue
                if not data["ExactTS"]+data["M1"] in history:
                    ss = PrettyStrings.formatTime(data) + " | "
                    ss += PrettyStrings.formatString(multiple_replace({"_&1_": "'", "_&2_": "=", "_&3_": ";"}, data["M1"]), 128)
                    ss += "\n"
                    s = ss + s
                    history.append(data["ExactTS"]+data["M1"])
            if len(s) > 0:
                print s.strip()
            time.sleep(0.5)



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
        jobID = self.getJobID() # pylint: disable-msg=E1101
        logDir = JEMConfig.MON_LOG_DIR + os.sep + Utils.escapeJobID(jobID)
        try:
            os.system("less " + logDir + os.sep + "JEMganga-Listener.log")
        except:
            outlogger.info("No live monitor log available")
            outlogger.debug("cause: " + str(sys.exc_info()[0]) + ": " + str(sys.exc_info()[1]))


    def plotMetrics(self):
        """
        This method tries to plot the system information data using gnuplot.
        """
        if self.__checkStatus():
            # gather metrics from all RESOURCE messages
            data = self.__seekJMDinfo("RESOURCE", 0, 0, True)
            if len(data) == 0:
                return

            t = "Recorded system metrics"
            if data[0].has_key("WN"):
                t += " on " + data[0]["WN"]

            data1 = [[], []]
            meta1 = [
                        {"title": "load"},
                        {"title": "mem", "axes": "x1y2"}
                    ]
            conf1 = {
                        "xtime": True, "title": t + ": Performance",
                        "ylabel": "Load (1 min avg)",
                        "y2label": "Free memory [Mb]",
                        "yrange": "[0:*]", "y2range": "[0:*]"
                    }

            data2 = [[],[],[]]
            meta2 = [
                        {"title": "working directory"},
                        {"title": "temp directory"},
                        {"title": "swap directory"}
                    ]
            conf2 = {
                        "xtime": True, "title": t + ": Disk space",
                        "ylabel": "Free disk space [Mb]",
                        "yrange": "[0:*]"
                    }

            for l in data:
                try:
                    data1[0] += [(l["ExactTS"], l["Load"])]
                    data1[1] += [(l["ExactTS"], l["Mem"])]

                    data2[0] += [(l["ExactTS"], l["Workdir"])]
                    data2[1] += [(l["ExactTS"], l["Tmp"])]
                    data2[2] += [(l["ExactTS"], l["Swap"].strip())]
                except:
                    pass

            Utils.drawPlots(data1, meta1, conf1)
            Utils.drawPlots(data2, meta2, conf2)


    def waitForRealStart(self):
        """
        This method waits for the user application to start on the WN; it may be aborted
        with <return>.
        """
        def keyPressed():
            return select.select([sys.stdin], [], [], 0) == ([sys.stdin], [], [])

        try:
            print "waiting for user application to start... (to abort just press <return>)"
            while not self.userAppRunning and not keyPressed():
                time.sleep(0.5)
        except:
            pass


    def watch(self, overrideJobId = None):
        """
        Start a watcher thread for this job that will inform us automatically when
        the user application on the worker node has started running. This is called
        automatically on submit, so you only need to call it by yourself if you
        quit Ganga during a job run and it hasn't begun running yet when you restar-
        ted Ganga.
        """
        if self.watcherThread:
            self.abortWatch()

        job = self.getJobObject()
        gangaID = job.id # pylint: disable-msg=E1101
        if overrideJobId:
            jobID = overrideJobId
        else:
            jobID = self.getJobID()

        self.watcherThread = self.JEMWatcherThread(self, jobID, gangaID, self.jmdfile)
        self.watcherThread.start()


    def abortWatch(self):
        """
        Stop the watcher thread for this job, if any.
        """
        try:
            if self.watcherThread:
                self.watcherThread.stop()
                self.watcherThread = None
        except:
           pass


    ####################################################################################################################
    ### methods that are exported to GPI, but usually not called by the user...

    def _getListenerPid(self):
        return self.pid


    def _getServerPid(self):
        return self.__getServerPid()


    def _getServerPort(self):
        return self.port


    def _getServerStatus(self):
        return self.__checkStatus(True)


    def _hasUserAppStarted(self):
        return self.userAppRunning


    def _hasUserAppExited(self):
        return self.userAppExited


    def _getTransmissionStats(self):
        try:
            return self.__transmissionStats()
        except:
            return []


    ####################################################################################################################
    ### methods not exported to GPI

    def onBegunToReceiveMonitoringData(self):
        """
        This hook is called by the watcher thread as soon as the first data is received
        for this job.
        """
        job = self.getJobObject()
        jobGangaID = job.id # pylint: disable-msg=E1101
        self.userAppRunning = True
        outlogger.info("Begun to receive monitoring data for job " + str(jobGangaID))


    def onWatcherThink(self):
        """
        This hook is called by the watcher thread every 5 seconds from the moment the
        first data is received on.
        """
        job = self.getJobObject()

        try:
            jobGangaID = str(job.id) # pylint: disable-msg=E1101

            l = self.__seekJMDinfo("JOBSTATE", 1, 1) # pylint: disable-msg=W0212
            if len(l) and len(l[0]):
                if l[0]["Status"] == "FINISHED":
                    self.userAppExited = True
        except:
            pass

        if self.userAppExited:
            outlogger.info("User application of job " + str(jobGangaID) +\
                           " seems to have finished! Now waiting for the middleware...")
            self.watcherThread.stop()


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

        if isinstance(attribute, JEMCTraceOptions):
            #return "CTracer options; access via info.monitor.ctracer"
            return Anonymous()
        elif isinstance(attribute, JEMAdvancedOptions):
            #return "advanced options; access via info.monitor.advanced"
            return Anonymous()

        return str(attribute)


    def __checkStatus(self, onlyReport = False):
        """
        Checks JEM's overall health.
        Returns True if JEM is enabled and working correctly. Otherwise
        returns False. Can utter a warning or error message, for example
        for the following reasons:

        - JEM is disabled globally
        - JEM is disabled for this job
        - the JEM library couldn't be found or loaded
        - the realtime listener process can't be found / is inactive
          (note: this is normal for finished jobs!)
        """
        j = self.getJobObject() # pylint: disable-msg=E1101

        if not JEMloader.INITIALIZED:
            if onlyReport:
                return "disabled"
            outlogger.info("Monitoring is globally disabled. No monitoring data is available.")
            return False
        if not self.enabled: # pylint: disable-msg=E1101
            if onlyReport:
                return "disabled"
            outlogger.info("Monitoring is disabled for this job. No monitoring data is available.")
            return False

        if j.status not in ('completed', 'failed', 'killed', 'removed'):
            if not self.realtime or not jemconfig['JEM_ENABLE_REALTIME']:
                if onlyReport:
                    return "disabled"
                outlogger.info("Realtime monitoring is disabled. Monitoring data will only be available in the output sandbox.")
                return False
            if self.pid == 0:
                if self.getJobObject().status == 'new': # pylint: disable-msg=E1101
                    if onlyReport:
                        return "not yet started"
                    outlogger.info("Job has not been submitted yet. No monitoring data is available.")
                else:
                    if onlyReport:
                        return "error"
                    outlogger.info("No monitoring process started (check configuration). No monitoring data is available.")
                return False

        if not os.path.exists(self.jmdfile):
            if onlyReport:
                return "waiting"
            outlogger.info("No monitoring data was received yet.")
            return False

        # To potentially utter a warning, we check the Listener status...
        if onlyReport:
            return self.__isListenerActive(True)

        self.__isListenerActive()
        return True


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


    def __getServerPid(self):
        pid = str(self.pid) # pylint: disable-msg=E1101
        job = self.getJobObject()
        jobID = self.getJobID() # pylint: disable-msg=E1101

        if job.status not in ['running','submitted']: # pylint: disable-msg=E1101
            return 0

        pids, ppids, cmds = self.__getChildProcesses()

        # Check if all processes are running
        if not pid in pids:
            if pid != "0":
                logger.debug("in __getServerPid: pid not in pids (%s not in %s)" % (str(pid), str(pids)))
            return 0
        else:
            z = pids.index(pid)
            if cmds[z].find("[python] <defunct>") != -1:
                logger.debug("in __getServerPid: server command status includes <defunct>")
                return 0
            else:
                for z,p in enumerate(ppids):
                    if p == pid:
                        # okay, well, let's assume SOME server is OK...
                        if (cmds[z].find("HTTPSServer") != -1 or cmds[z].find("RGMAServer") != -1 or cmds[z].find("StompServer") != -1)\
                           and cmds[z].find("[python] <defunct>") == -1:
                            return int(pids[z])
                logger.debug("in __getServerPid: server commands didn't include HTTPSServer or RGMAServer, or did include <defunct>")
                return 0


    def __isListenerActive(self, onlyReport = False):
        """
        Checks if the R-GMA/HTTPS Server and job listener are active
        """

        # not the finest solution, but it works and is quite fast!
        pid = str(self.pid) # pylint: disable-msg=E1101
        job = self.getJobObject()
        jobID = self.getJobID() # pylint: disable-msg=E1101
        logDir = JEMConfig.MON_LOG_DIR + os.sep + Utils.escapeJobID(jobID)

        # check the job state
        if job.status not in ['running','submitted']: # pylint: disable-msg=E1101
            if onlyReport:
                return "finished"
            outlogger.info("Job execution finished. No new data will be received.")
            return True

        # check if RGMA/HTTPS started correctly
        stdoutFileName = logDir + os.sep + 'stdout.log'
        if os.path.exists(stdoutFileName):
            fd = open(stdoutFileName,'r')
            stdout = fd.read()

            rgma = True
            https = True
            tcp = True
            stomp = True

            # try to find some strings to check if rgma is active
            if stdout.find('Can not create server process R-GMA') != -1:
                rgma = False
            if stdout.find('R-GMA package not found') != -1:
                rgma = False

            # try to find some strings to check if https is active
            if stdout.find('Can not create server process HTTPS') != -1:
                https = False

            # try to find some strings to check if TCP is active
            if stdout.find('Can not create server process TCP') != -1:
                tcp = False

            # try to find some strings to check if TCP is active
            if stdout.find('Can not create server process STOMP') != -1:
                stomp = False

            if not (rgma or https or tcp or stomp):
                if onlyReport:
                    return "error"
                logger.error('Failed to start JEMs listening server(s). Monitoring is not active.')
                return False

        pids, ppids, cmds = self.__getChildProcesses()

        # Check if all processes are running
        if not pid in pids:
            if not onlyReport:
                if not self.userAppExited:
                    outlogger.warn("JEM livemonitor is not running. No new data will be received (Displayed data may be outdated).")
            else:
                return "error"
            #logger.debug("cause: not in there. pids: " + str(pids))
        else:
            z = pids.index(pid)
            if cmds[z].find("[python] <defunct>") != -1:
                if not onlyReport:
                    if not self.userAppExited:
                        outlogger.warn("JEM livemonitor is not running. No new data will be received (Displayed data may be outdated).")
                else:
                    return "error"
                #logger.debug("cause: <defunct>. pids: " + str(pids))
            else:
                for z,p in enumerate(ppids):
                    if p == pid:
                        if cmds[z].find("HTTPSServer") != -1:
                            if cmds[z].find("<defunct>") == -1:
                                if onlyReport:
                                    return "OK"
                                return True
                            else:
                                if onlyReport:
                                    return "error"
                                break
                        if cmds[z].find("RGMAServer") != -1:
                            if cmds[z].find("<defunct>") == -1:
                                if onlyReport:
                                    return "OK"
                                return True
                            else:
                                if onlyReport:
                                    return "error"
                                break
                        if cmds[z].find("StompServer") != -1:
                            if cmds[z].find("<defunct>") == -1:
                                if onlyReport:
                                    return "OK"
                                return True
                            else:
                                if onlyReport:
                                    return "error"
                                break
                if not self.userAppExited:
                    outlogger.warn("JEM livemonitor is not running. No new data will be received (Displayed data may be outdated).")
                else:
                    outlogger.info("Job execution on the worker node has finished. No new data will be received.")
        if onlyReport:
            return "unknown"
        return True


    def __getChildProcesses(self):
        """
        Find the child-processes of LiveMonitoring.py (Server, PipePublisher-launched stuff, etc).
        """

        # prepare for grep
        job = self.getJobObject()
        jobID = self.getJobID()
        logDir = JEMConfig.MON_LOG_DIR + os.sep + Utils.escapeJobID(jobID)

        # dont delete the width parameter, otherwise the grep command will fail due to line length...
        cmd = "ps --width 1000 -eo user,pid,ppid,command | grep " + getpass.getuser() + " | awk '{ print $1, $2, $3, $4,$5,$6,$7,$8,$9 }'"

        # save results to tmp file
        if not os.path.exists(logDir):
            os.makedirs(logDir, 0777)
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


    def __parseJMDline(self, line):
        result = {}
        try:
            nodes = line.split(";")
            for n in nodes:
                try:
                    k,v = n.split("=")
                    result[k] = v
                except:
                    pass
        except:
            return {}
        return result


    def __seekJMDinfo(self, tag, n, start, ascending = False):
        """
        read the 'n' first/last lines after 'start' lines of the type 'tag' from the JMD file

        @param tag: The type of messages to look for
        @param n: The number of messages to read
        @param start: The start-th message to start at
        @param ascending: Wether to start at the top or bottom of the logfile
        """
        if ascending:
            fd = open(self.jmdfile, "r")
        else:
            fd = ropen(self.jmdfile)

        jmdlines = []
        z = 1
        while 1:
            try:
                line = fd.readline()
                if len(line) == 0:
                    break
                line = self.__parseJMDline(line)
                try:
                    if line["Type"] == tag:
                        z += 1
                        if z > start:
                            jmdlines += [line]
                            if n != 0 and z - start >= n:
                                break
                except:
                    continue
            except StopIteration:
                break

        try:
            fd.close()
        except:
            pass

        return jmdlines


    def __transmissionStats(self):
        fd = ropen(self.jmdfile)
        stats = {"Tb": 0, "Tc": 0, "Rc": 0, "Ec": 0, "Pc": 0, "Cc": 0}

        while 1:
            try:
                line = fd.readline()
                ll = len(line)
                if ll == 0:
                    break
                elif ll == 1:
                    continue

                stats["Tb"] += ll
                stats["Tc"] += 1

                line = self.__parseJMDline(line)

                if line.has_key("Type"):
                    if line["Type"] == "RESOURCE":
                        stats["Rc"] += 1
                    elif line["Type"] == "EXCEPTION":
                        stats["Ec"] += 1
                    elif line["Type"][3:] == "PEEKLINE":
                        stats["Pc"] += 1
                    else:
                        stats["Cc"] += 1
            except StopIteration:
                break

        try:
            fd.close()
        except:
            pass

        # in case the watcher thread didn't notice...
        if stats["Tc"] > 0 and not self.userAppRunning:
            self.onBegunToReceiveMonitoringData()

        return stats


    def __decompressTar(self, logfilePath):
        """
        Decompresses the jem monitoring log files into the job output directory
        """
        logfileName = str(logfilePath) + WNConfig.COMPLETE_LOG
        if not os.path.exists(logfileName):
            return False
        else:
            try:
                os.system('cd ' + logfilePath + '; tar -xzf ' + logfileName)
                return True
            except:
                return False


    ####################################################################################################################
    ### private types

    class JEMWatcherThread(GangaThread):
        def __init__(self, jemObject, jobID, gangaID, jmdfile):
            GangaThread.__init__(self, name='JEM watcher thread for job #' + str(gangaID))
            self.jemObject = jemObject
            self.gangaID = gangaID
            self.jmdfile = jmdfile
            self.jobID = jobID
            self.gotData = False
            self.setDaemon(True)

        def __deepcopy__(self, memento):
            return None # te he he!

        def run(self):
            logger.debug("started watcher thread for job " + str(self.gangaID))
            z = 0
            while not self.should_stop():
                time.sleep(0.1)
                z += 1
                if not self.gotData:
                    if z >= 10:
                        z = 0
                        try:
                            info = os.stat(self.jmdfile)
                            if info.st_size > 0:
                                self.gotData = True
                                self.jemObject.onBegunToReceiveMonitoringData()
                        except:
                            pass
                else:
                    if z >= 50:
                        z = 0
                        self.jemObject.onWatcherThink()

            logger.debug("watcher thread of job " + str(self.gangaID) + " exits")
            self.unregister()
