# System Imports
import os
import re
import inspect
import getpass

from Ganga.Utility.ColourText import ANSIMarkup, overview_colours


# Global Functions
def getEnvironment(config = None):
    return {}

def getLCGRootPath():

    lcg_release_areas = {'afs' : '/afs/cern.ch/sw/lcg/releases/LCG_79',
    'cvmfs' : '/cvmfs/lhcb.cern.ch/lib/lcg/releases/LCG_79'}

    ## CAUTION This could be sensitive to mixed AFS/CVMFS running but I doubt this setup is common or likely
    myCurrentPath = os.path.abspath(inspect.getfile(inspect.currentframe()))

    if myCurrentPath[:4].upper() == '/AFS':
        return lcg_release_areas['afs']
    elif myCurrentPath[:6].upper() == '/CVMFS':
        return lcg_release_areas['cvmfs']
    else:
        return ''

# ------------------------------------------------
# store Ganga version based on new git tag for this file
_gangaVersion = '$Name: 6.1.20 $'

# [N] in the pattern is important because it prevents CVS from expanding the pattern itself!
r = re.compile(r'\$[N]ame: (?P<version>\S+) \$').match(_gangaVersion)
if r:
    _gangaVersion = r.group('version')
else:
    _gangaVersion = "SVN_TRUNK"

# store a path to Ganga libraries
_gangaPythonPath = os.path.dirname(os.path.dirname(__file__))


# grab the hostname
try:
    from Ganga.Utility.util import hostname
    hostname = hostname()
except Exception as x:  # fixme: use OSError instead?
    hostname = 'localhost'


# ------------------------------------------------
# Setup all configs for this module
from Ganga.Utility.Config import makeConfig, getConfig, expandvars


# ------------------------------------------------
# Logging
log_config = makeConfig("Logging", """control the messages printed by Ganga
The settings are applied hierarchically to the loggers. Ganga is the name of the top-level logger which
applies by default to all Ganga.* packages unless overriden in sub-packages.
You may define new loggers in this section.
The log level may be one of: CRITICAL ERROR WARNING INFO DEBUG
""", is_open=True)

# FIXME: Ganga WARNING should be turned into INFO level when the messages
# are reviewed in all the code
log_config.addOption('Ganga', "INFO", "top-level logger")
log_config.addOption('Ganga.Runtime.bootstrap', "INFO", 'FIXME')
log_config.addOption('Ganga.GPIDev', "INFO", "logger of Ganga.GPIDev.* packages")
log_config.addOption('Ganga.Utility.logging', "WARNING", "logger of the Ganga logging package itself (use with care!)")
log_config.addOption('_format', "NORMAL", "format of logging messages: TERSE,NORMAL,VERBOSE,DEBUG")
log_config.addOption('_colour', True, "enable ASCII colour formatting of messages e.g. errors in red")
log_config.addOption('_logfile', "~/.ganga.log", "location of the logfile")
log_config.addOption('_logfile_size', 100000,
                 "the size of the logfile (in bytes), the rotating log will never exceed this file size")  # 100 K
log_config.addOption('_interactive_cache', True,
                 'if True then the cache used for interactive sessions, False disables caching')
log_config.addOption('_customFormat', "", "custom formatting string for Ganga logging\n e.g. '%(name)-35s: %(levelname)-8s %(message)s'")

# test if stomp.py logging is already set
if 'stomp.py' in log_config:
    pass  # config['stomp.py']
else:
    # add stomp.py option to Logging configuration
    log_config.addOption('stomp.py', 'CRITICAL', 'logger for stomp.py external package')

# ------------------------------------------------
# System
# the system variables (such as VERSION) are put to DEFAULTS section of the config module
# so you can refer to them in the config file
# additionally they will be visible in the (write protected) [System]
# config module - write protection is done in bootstrap at present though this should be changed
import Ganga.Utility.files
config_path = Ganga.Utility.files.expandfilename(
    os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), '..')) )
sys_config = makeConfig('System', "parameters of this ganga session (read-only)", cfile=False)
sys_config.addOption('GANGA_VERSION', _gangaVersion, '')
sys_config.addOption('GANGA_PYTHONPATH', _gangaPythonPath, 'location of the ganga core packages')
sys_config.addOption('GANGA_CONFIG_PATH', config_path + '/','site/group specific configuration files as specified by --config-path or GANGA_CONFIG_PATH variable')
sys_config.addOption('GANGA_CONFIG_FILE', os.path.expanduser('~/.gangarc'), 'current user config file used')
sys_config.addOption('GANGA_HOSTNAME', hostname, 'local hostname where ganga is running')

# ------------------------------------------------
# Configuration
# the SCRIPTS_PATH must be initialized before the config files are loaded
# for the path to be correctly prepended
conf_config = makeConfig("Configuration", "global configuration parameters.\nthis is a catch all section.")
conf_config.addOption('SCRIPTS_PATH', 'Ganga/scripts', """the search path to scripts directory.
When running a script from the system shell (e.g. ganga script) this path is used to search for script""")

conf_config.addOption('LOAD_PATH', '', "the search path for the load() function")
conf_config.addOption('RUNTIME_PATH', '',
"""path to runtime plugin packages where custom handlers may be added.
Normally you should not worry about it.
If an element of the path is just a name (like in the example below)
then the plugins will be loaded using current python path. This means that
some packages such as GangaTest may be taken from the release area.""",
        examples="RUNTIME_PATH = GangaGUIRUNTIME_PATH = /my/SpecialExtensions:GangaTest ")

conf_config.addOption('TextShell', 'IPython', """ The type of the interactive shell: IPython (cooler) or Console (limited)""")
conf_config.addOption('StartupGPI', '', 'block of GPI commands executed at startup')
conf_config.addOption('ReleaseNotes', True, 'Flag to print out the relevent subsection of release notes for each experiment at start up')
conf_config.addOption('gangadir', expandvars(None, '~/gangadir'),
                 'Location of local job repositories and workspaces. Default is ~/gangadir but in somecases (such as LSF CNAF) this needs to be modified to point to the shared file system directory.', filter=Ganga.Utility.Config.expandvars)
conf_config.addOption(
    'repositorytype', 'LocalXML', 'Type of the repository.', examples='LocalXML')
conf_config.addOption('workspacetype', 'LocalFilesystem',
                 'Type of workspace. Workspace is a place where input and output sandbox of jobs are stored. Currently the only supported type is LocalFilesystem.')
conf_config.addOption('user', getpass.getuser(),
    'User name. The same person may have different roles (user names) and still use the same gangadir. Unless explicitly set this option defaults to the real user name.')
conf_config.addOption('resubmitOnlyFailedSubjobs', True,
                 'If TRUE (default), calling job.resubmit() will only resubmit FAILED subjobs. Note that the auto_resubmit mechanism will only ever resubmit FAILED subjobs.')
conf_config.addOption('SMTPHost', 'localhost', 'The SMTP server for notification emails to be sent, default is localhost')
conf_config.addOption('deleteUnusedShareDir', 'always',
                 'If set to ask the user is presented with a prompt asking whether Shared directories not associated with a persisted Ganga object should be deleted upon Ganga exit. If set to never, shared directories will not be deleted upon exit, even if they are not associated with a persisted Ganga object. If set to always (the default), then shared directories will always be deleted if not associated with a persisted Ganga object.')

conf_config.addOption('autoGenerateJobWorkspace', False, 'Autogenerate workspace dirs for new jobs')

# add named template options
conf_config.addOption('namedTemplates_ext', 'tpl',
                 'The default file extension for the named template system. If a package sets up their own by calling "establishNamedTemplates" from python/Ganga/GPIDev/Lib/Job/NamedJobTemplate.py in their ini file then they can override this without needing the config option')
conf_config.addOption('namedTemplates_pickle', False,
                 'Determines if named template system stores templates in pickle file format (True) or in the Ganga streamed object format (False). By default streamed object format which is human readable is used. If a package sets up their own by calling "establishNamedTemplates" from python/Ganga/GPIDev/Lib/Job/NamedJobTemplate.py in their ini file then they can override this without needing the config option')

# add server options
conf_config.addOption('ServerPort', 434343, 'Port for the Ganga server to listen on')
conf_config.addOption('ServerTimeout', 60, 'Timeout in minutes for auto-server shutdown')
conf_config.addOption('ServerUserScript', "", "Full path to user script to call periodically. The script will be executed as if called within Ganga by 'execfile'.")
conf_config.addOption('ServerUserScriptWaitTime', 300, "Time in seconds between executions of the user script")

conf_config.addOption('confirm_exit', 1, 'Ask the user on exit if we should exit, (this is passed along to IPython)')
conf_config.addOption('force_start', False, 'Ignore disk checking on startup')

conf_config.addOption('DiskIOTimeout', 45, 'Time in seconds before a ganga session (lock file) is treated as a zombie and removed')

# runtime warnings issued by the interpreter may be suppresed
conf_config.addOption('IgnoreRuntimeWarnings', False, "runtime warnings issued by the interpreter may be suppresed")

conf_config.addOption('UsageMonitoringMSG', True,
                 "enable usage monitoring through MSG server defined in MSGMS configuration")
conf_config.addOption('Batch', 'LSF', 'default batch system')

conf_config.addOption('AutoStartReg', True, 'AutoStart the registries, needed to access any jobs in registry therefore needs to be True for 99.999% of use cases')

# ------------------------------------------------
# IPython
ipython_config = makeConfig('TextShell_IPython', '''IPython shell configuration
See IPython manual for more details:
http://ipython.scipy.org/doc/manual''')
ipython_config.addOption('args', "['-colors','LightBG', '-autocall','0', '-pprint']", 'Options to be passed to ipython for initialization')

# ------------------------------------------------
# Shell
makeConfig("Shell", "configuration parameters for internal Shell utility.")

# ------------------------------------------------
# Queues
queues_config = makeConfig("Queues", "configuration section for the queues")
queues_config.addOption('Timeout', None, 'default timeout for queue generated processes')
queues_config.addOption('NumWorkerThreads', 3, 'default number of worker threads in the queues system')

# ------------------------------------------------
# MSGMS
msgms_config = makeConfig('MSGMS', 'Settings for the MSGMS monitoring plugin. Cannot be changed ruding the interactive Ganga session.')
msgms_config.addOption('server', 'dashb-mb.cern.ch', 'The server to connect to')
msgms_config.addOption('port', 61113, 'The port to connect to')
msgms_config.addOption('username', 'ganga', '')
msgms_config.addOption('password', 'analysis', '')
msgms_config.addOption('message_destination', '/queue/ganga.status', '')
msgms_config.addOption('usage_message_destination', "/queue/ganga.usage", '')
msgms_config.addOption('job_submission_message_destination', "/queue/ganga.jobsubmission", '')

# ------------------------------------------------
# Plugins
default_plugins_cfg = makeConfig('Plugins', '''General control of plugin mechanism.
Set the default plugin in a given category.
For example:
default_applications = DaVinci
default_backends = LCG
''')

# ------------------------------------------------
# GPI Semantics
gpi_config = makeConfig('GPI_Semantics',
                       'Customization of GPI behaviour. These options may affect the semantics of the Ganga GPI interface (what may result in a different behaviour of scripts and commands).')
gpi_config.addOption('job_submit_keep_going', False,
                    'Keep on submitting as many subjobs as possible. Option to j.submit(), see Job class for details')
gpi_config.addOption('job_submit_keep_on_fail', False,
                    'Do not revert job to new status even if submission failed. Option to j.submit(), see Job class for details')

# ------------------------------------------------
# PollThread
poll_config = makeConfig('PollThread', 'background job status monitoring and output retrieval')
poll_config.addOption('repeat_messages', False, 'if 0 then log only once the errors for a given backend and do not repeat them anymore')
poll_config.addOption('autostart', True, 'enable monitoring automatically at startup, in script mode monitoring is disabled by default, in interactive mode it is enabled', type=type(True))  # enable monitoring on startup
poll_config.addOption('autostart_monThreads', True, 'enable populating of the monitoring worker threads')
poll_config.addOption('base_poll_rate', 2, 'internal supervising thread', hidden=1)
poll_config.addOption('MaxNumResubmits', 5, 'Maximum number of automatic job resubmits to do before giving')
poll_config.addOption('MaxFracForResubmit', 0.25, 'Maximum fraction of failed jobs before stopping automatic resubmission')
poll_config.addOption('update_thread_pool_size', 5, 'Size of the thread pool. Each threads monitors a specific backaend at a given time. Minimum value is one, preferably set to the number_of_backends + 1')
poll_config.addOption('default_backend_poll_rate', 30, 'Default rate for polling job status in the thread pool. This is the default value for all backends.')
poll_config.addOption('Local', 10, 'Poll rate for Local backend.')
poll_config.addOption('LCG', 30, 'Poll rate for LCG backend.')
poll_config.addOption('Condor', 30, 'Poll rate for Condor backend.')
poll_config.addOption('gLite', 30, 'Poll rate for gLite backend.')
poll_config.addOption('LSF', 20, 'Poll rate for LSF backend.')
poll_config.addOption('PBS', 20, 'Poll rate for PBS backend.')
poll_config.addOption('Dirac', 50, 'Poll rate for Dirac backend.')
poll_config.addOption('Panda', 50, 'Poll rate for Panda backend.')

# Note: the rate of this callback is actually
# MAX(base_poll_rate,callbacks_poll_rate)
poll_config.addOption('creds_poll_rate', 30, "The frequency in seconds for credentials checker")
poll_config.addOption('diskspace_poll_rate', 30, "The frequency in seconds for free disk checker")
poll_config.addOption('DiskSpaceChecker', "", "disk space checking callback. This function should return False when there is no disk space available, True otherwise")
poll_config.addOption('max_shutdown_retries', 5, 'OBSOLETE: this option has no effect anymore')
poll_config.addOption('numParallelJobs', 50, 'Number of Jobs to update the status for in parallel')

poll_config.addOption('forced_shutdown_policy', 'session_type',
                 'If there are remaining background activities at exit such as monitoring, output download Ganga will attempt to wait for the activities to complete. You may select if a user is prompted to answer if he wants to force shutdown ("interactive") or if the system waits on a timeout without questions ("timeout"). The default is "session_type" which will do interactive shutdown for CLI and timeout for scripts.')
poll_config.addOption('forced_shutdown_timeout', 60,
                 "Timeout in seconds for forced Ganga shutdown in batch mode.")
poll_config.addOption('forced_shutdown_prompt_time', 10,
                 "User will get the prompt every N seconds, as specified by this parameter.")
poll_config.addOption('forced_shutdown_first_prompt_time', 5,
                 "User will get the FIRST prompt after N seconds, as specified by this parameter. This parameter also defines the time that Ganga will wait before shutting down, if there are only non-critical threads alive, in both interactive and batch mode.")

import sys
poll_config.addOption('HeartBeatTimeOut', sys.maxint, 'Time before the user gets the warning that a thread has locked up due to failing to update the heartbeat attribute')

# ------------------------------------------------
# Feedback
feedback_config = makeConfig('Feedback', 'Settings for the Feedback plugin. Cannot be changed during the interactive Ganga session.')
feedback_config.addOption('uploadServer', 'http://gangamon.cern.ch/django/errorreports', 'The server to connect to')

# ------------------------------------------------
# Associations
assoc_config = makeConfig(
    "File_Associations", 'Default associations between file types and file-viewing commands. The name identifies the extension and the value the commans. New extensions can be added. A single & after the command indicates that the process will be started in the background. A && after the command indicates that a new terminal will be opened and the command executed in that terminal.', is_open=True)

assoc_config.addOption("newterm_command", "xterm",
                 'Command for opening a new terminal (xterm, gnome-terminal, ...')
assoc_config.addOption("newterm_exeopt", "-e",
                 'Option to give to a new terminal to tell it to execute a command.')
assoc_config.addOption(
    "listing_command", "ls -ltr", 'Command for listing the content of a directory')
assoc_config.addOption('fallback_command', 'less',
                 'Default command to use if there is no association with the file type')
assoc_config.addOption('htm', 'firefox &', 'Command for viewing html files.')
assoc_config.addOption('html', 'firefox &', 'Command for viewing html files.')
assoc_config.addOption('root', 'root.exe &&', 'Command for opening ROOT files.')
assoc_config.addOption('tar', 'file-roller &', 'Command for opening tar files.')
assoc_config.addOption('tgz', 'file-roller &', 'Command for opening tar files.')

# ------------------------------------------------
# Root
root_config = makeConfig('ROOT', "Options for Root backend")
## Not needed when we can't do option substitution internally but support it at the .gangarc level!!!!! 27-09-2015 rcurrie
#config.addOption('lcgpath', getLCGRootPath(), 'Path of the LCG release that the ROOT project and it\'s externals are taken from')
root_config.addOption('arch', 'x86_64-slc6-gcc48-opt', 'Architecture of ROOT')
## Auto-Interporatation doesn't appear to work when setting the default value
#config.addOption('location', '${lcgpath}/ROOT/${version}/${arch}/', 'Location of ROOT')
root_config.addOption('location', '%s/ROOT/6.04.02/x86_64-slc6-gcc48-opt' % getLCGRootPath(), 'Location of ROOT')
root_config.addOption('path', '', 'Set to a specific ROOT version. Will override other options.')
## Doesn't appear to work see above ^^^
#config.addOption('pythonhome', '${lcgpath}/Python/${pythonversion}/${arch}/','Location of the python used for execution of PyROOT script')
root_config.addOption('pythonhome', '%s/Python/2.7.9.p1/x86_64-slc6-gcc48-opt' % getLCGRootPath(), 'Location of the python used for execution of PyROOT script')
root_config.addOption('pythonversion', '2.7.9.p1', "Version number of python used for execution python ROOT script")
root_config.addOption('version', '6.04.02', 'Version of ROOT')

# ------------------------------------------------
# Local
local_config = makeConfig('Local', 'parameters of the local backend (jobs in the background on localhost)')
local_config.addOption('remove_workdir', True, 'remove automatically the local working directory when the job completed')
local_config.addOption('location', None, 'The location where the workdir will be created. If None it defaults to the value of $TMPDIR')

# ------------------------------------------------
# LCG
lcg_config = makeConfig('LCG', 'LCG/gLite/EGEE configuration parameters')
#gproxy_config = getConfig('GridProxy_Properties')

# set default values for the configuration parameters
lcg_config.addOption(
    'EDG_ENABLE', False, 'enables/disables the support of the EDG middleware')

lcg_config.addOption('EDG_SETUP', '/afs/cern.ch/sw/ganga/install/config/grid_env_auto.sh',
                 'sets the LCG-UI environment setup script for the EDG middleware',
                 filter=Ganga.Utility.Config.expandvars)

lcg_config.addOption(
    'GLITE_ENABLE', True, 'Enables/disables the support of the GLITE middleware')

lcg_config.addOption('GLITE_SETUP', '/afs/cern.ch/sw/ganga/install/config/grid_env_auto.sh',
                 'sets the LCG-UI environment setup script for the GLITE middleware',
                 filter=Ganga.Utility.Config.expandvars)

lcg_config.addOption('VirtualOrganisation', 'dteam',
                 'sets the name of the grid virtual organisation')

lcg_config.addOption('ConfigVO', '', 'sets the VO-specific LCG-UI configuration script for the EDG resource broker',
                 filter=Ganga.Utility.Config.expandvars)

lcg_config.addOption('Config', '', 'sets the generic LCG-UI configuration script for the GLITE workload management system',
                 filter=Ganga.Utility.Config.expandvars)

lcg_config.addOption(
    'AllowedCEs', '', 'sets allowed computing elements by a regular expression')
lcg_config.addOption(
    'ExcludedCEs', '', 'sets excluded computing elements by a regular expression')

lcg_config.addOption(
    'GLITE_WMS_WMPROXY_ENDPOINT', '', 'sets the WMProxy service to be contacted')
lcg_config.addOption('GLITE_ALLOWED_WMS_LIST', [], '')

lcg_config.addOption('MyProxyServer', 'myproxy.cern.ch', 'sets the myproxy server')
lcg_config.addOption('RetryCount', 3, 'sets maximum number of job retry')
lcg_config.addOption(
    'ShallowRetryCount', 10, 'sets maximum number of job shallow retry')

lcg_config.addOption(
    'Rank', '', 'sets the ranking rule for picking up computing element')
lcg_config.addOption('ReplicaCatalog', '', 'sets the replica catalogue server')
lcg_config.addOption('StorageIndex', '', 'sets the storage index')

lcg_config.addOption(
    'DefaultSE', 'srm.cern.ch', 'sets the default storage element')
lcg_config.addOption('DefaultSRMToken', '',
                 'sets the space token for storing temporary files (e.g. oversized input sandbox)')
lcg_config.addOption(
    'DefaultLFC', 'prod-lfc-shared-central.cern.ch', 'sets the file catalogue server')
lcg_config.addOption('BoundSandboxLimit', 10 * 1024 * 1024,
                 'sets the size limitation of the input sandbox, oversized input sandbox will be pre-uploaded to the storage element specified by \'DefaultSE\' in the area specified by \'DefaultSRMToken\'')

lcg_config.addOption('Requirements', 'Ganga.Lib.LCG.LCGRequirements',
                 'sets the full qualified class name for other specific LCG job requirements')

lcg_config.addOption('SandboxCache', 'Ganga.Lib.LCG.LCGSandboxCache',
                 'sets the full qualified class name for handling the oversized input sandbox')

lcg_config.addOption('GliteBulkJobSize', 50,
                 'sets the maximum number of nodes (i.e. subjobs) in a gLite bulk job')

lcg_config.addOption('SubmissionThread', 10,
                 'sets the number of concurrent threads for job submission to gLite WMS')

lcg_config.addOption(
    'SubmissionTimeout', 300, 'sets the gLite job submission timeout in seconds')

lcg_config.addOption('StatusPollingTimeout', 300,
                 'sets the gLite job status polling timeout in seconds')

lcg_config.addOption('OutputDownloaderThread', 10,
                 'sets the number of concurrent threads for downloading job\'s output sandbox from gLite WMS')

lcg_config.addOption('SandboxTransferTimeout', 60,
                 'sets the transfer timeout of the oversized input sandbox')

lcg_config.addOption(
    'JobLogHandler', 'WMS', 'sets the way the job\'s stdout/err are being handled.')

lcg_config.addOption('MatchBeforeSubmit', False,
                 'sets to True will do resource matching before submitting jobs, jobs without any matched resources will fail the submission')

lcg_config.addOption('IgnoreGliteScriptHeader', False,
                 'sets to True will load script-based glite-wms-* commands forcely with current python, a trick for 32/64 bit compatibility issues.')

# add ARC specific configuration options
#lcg_config.addOption('ArcInputSandboxBaseURI', '', 'sets the baseURI for getting the input sandboxes for the job')
#lcg_config.addOption('ArcOutputSandboxBaseURI', '', 'sets the baseURI for putting the output sandboxes for the job')
lcg_config.addOption('ArcWaitTimeBeforeStartingMonitoring', 240,
                 'Time in seconds to wait after submission before starting to monitor ARC jobs to ensure they are in the system')
lcg_config.addOption('ArcJobListFile', "~/.arc/gangajobs.xml",
                 'File to store ARC job info in when submitting and monitoring, i.e. argument to "-j" option in arcsub. Ganga default is different to ARC default (~/.arc/jobs.xml) to keep them separate.')
lcg_config.addOption('ArcConfigFile', "",
                 'Config file for ARC submission. Use to specify CEs, etc. Default is blank which will mean no config file is specified and the default (~/arc/client.conf) is used')
#lcg_config.addOption('ArcPrologue','','sets the prologue script')
#lcg_config.addOption('ArcEpilogue','','sets the epilogue script')

# add CREAM specific configuration options
lcg_config.addOption('CreamInputSandboxBaseURI', '',
                 'sets the baseURI for getting the input sandboxes for the job')
lcg_config.addOption('CreamOutputSandboxBaseURI', '',
                 'sets the baseURI for putting the output sandboxes for the job')
#lcg_config.addOption('CreamPrologue','','sets the prologue script')
#lcg_config.addOption('CreamEpilogue','','sets the epilogue script')

# ------------------------------------------------
# GridSimulator
gridsim_config = makeConfig('GridSimulator', 'Grid Simulator configuration parameters')

gridsim_config.addOption('submit_time', 'random.uniform(1,10)',
                 'python expression which returns the time it takes (in seconds) to complete the Grid.submit() command (also for subjob in bulk emulation)')
gridsim_config.addOption(
    'submit_failure_rate', 0.0, 'probability that the Grid.submit() method fails')

gridsim_config.addOption('cancel_time', 'random.uniform(1,5)',
                 'python expression which returns the time it takes (in seconds) to complete the Grid.cancel() command (also for subjob in bulk emulation)')
gridsim_config.addOption(
    'cancel_failure_rate', 0.0, 'probability that the Grid.cancel() method fails')

gridsim_config.addOption('status_time', 'random.uniform(1,5)',
                 'python expression which returns the time it takes (in seconds) to complete the status command (also for subjob in bulk emulation)')

gridsim_config.addOption('get_output_time', 'random.uniform(1,5)',
                 'python expression which returns the time it takes (in seconds) to complete the get_output command (also for subjob in bulk emulation)')

#config.addOption('bulk_submit_time','random.uniform(1,2)','python expression which returns the time it takes (in seconds) to complete the submission of a single job within the Grid.native_master_submit() command')
#config.addOption('bulk_submit_failure_rate',0.0,'probabilty that the Grid.native_master_submit() fails')

#config.addOption('bulk_cancel_time','random.uniform(1,2)','python expression which returns the time it takes (in seconds) to complete the cancellation of a single job within the Grid.native_master_cancel() command')
#config.addOption('bulk_cancel_failure_rate',0.0,'probabilty that the Grid.native_master_cancel() fails')

gridsim_config.addOption('job_id_resolved_time', 'random.uniform(1,2)',
                 'python expression which returns the time it takes (in seconds) to complete the resolution of all the id of a subjob (when submitted in bulk) this is the time the NODE_ID becomes available from the monitoring)')

#config.addOption('job_scheduled_time','random.uniform(10,20)', 'python expression which returns the time the job stays in the scheduled state')
#config.addOption('job_running_time','random.uniform(10,20)', 'python expression which returns the time the job stays in the running state')
gridsim_config.addOption('job_finish_time', 'random.uniform(10,20)',
                 'python expression which returns the time when the job enters the Done success or Failed state')
gridsim_config.addOption(
    'job_failure_rate', 0.0, 'probability of the job to enter the Failed state')

# ------------------------------------------------
# Condor
condor_config = makeConfig('Condor', 'Settings for Condor Batch system')

condor_config.addOption('query_global_queues', True,
                 "Query global condor queues, i.e. use '-global' flag")

# ------------------------------------------------
# LSF
lsf_config = makeConfig('LSF', 'internal LSF command line interface')

# fix bug #21687
lsf_config.addOption('shared_python_executable', False, "Shared PYTHON")

lsf_config.addOption('jobid_name', 'LSB_BATCH_JID', "Name of environment with ID of the job")
lsf_config.addOption('queue_name', 'LSB_QUEUE', "Name of environment with queue name of the job")
lsf_config.addOption('heartbeat_frequency', '30', "Heartbeat frequency config variable")

lsf_config.addOption('submit_str', 'cd %s; bsub %s %s %s %s', "String used to submit job to queue")
lsf_config.addOption('submit_res_pattern', '^Job <(?P<id>\d*)> is submitted to .*queue <(?P<queue>\S*)>',
                 "String pattern for replay from the submit command")

lsf_config.addOption('stdoutConfig', '-o %s/stdout', "String pattern for defining the stdout")
lsf_config.addOption('stderrConfig', '-e %s/stderr', "String pattern for defining the stderr")

lsf_config.addOption('kill_str', 'bkill %s', "String used to kill job")
lsf_config.addOption('kill_res_pattern',
                 '(^Job <\d+> is being terminated)|(Job <\d+>: Job has already finished)|(Job <\d+>: No matching job found)',
                 "String pattern for replay from the kill command")

tempstr = '''
'''
lsf_config.addOption('preexecute', tempstr,
                 "String contains commands executing before submiting job to queue")

tempstr = '''
def filefilter(fn):
  # FILTER OUT Batch INTERNAL INPUT/OUTPUT FILES:
  # 10 digits . any number of digits . err or out
  import re
  internals = re.compile(r'\d{10}\.\d+.(out|err)')
  return internals.match(fn) or fn == '.Batch.start'
'''
lsf_config.addOption('postexecute', tempstr, "String contains commands executing before submiting job to queue")
lsf_config.addOption('jobnameopt', 'J', "String contains option name for name of job in batch system")
lsf_config.addOption('timeout', 600, 'Timeout in seconds after which a job is declared killed if it has not touched its heartbeat file. Heartbeat is touched every 30s so do not set this below 120 or so.')

# ------------------------------------------------
# PBS
pbs_config = makeConfig('PBS', 'internal PBS command line interface')

pbs_config.addOption('shared_python_executable', False, "Shared PYTHON")

pbs_config.addOption('jobid_name', 'PBS_JOBID', "Name of environment with ID of the job")
pbs_config.addOption('queue_name', 'PBS_QUEUE', "Name of environment with queue name of the job")
pbs_config.addOption('heartbeat_frequency', '30', "Heartbeat frequency config variable")

pbs_config.addOption('submit_str', 'cd %s; qsub %s %s %s %s', "String used to submit job to queue")
pbs_config.addOption('submit_res_pattern', '^(?P<id>\d*)\.pbs\s*',
                 "String pattern for replay from the submit command")

pbs_config.addOption('stdoutConfig', '-o %s/stdout', "String pattern for defining the stdout")
pbs_config.addOption('stderrConfig', '-e %s/stderr', "String pattern for defining the stderr")

pbs_config.addOption('kill_str', 'qdel %s', "String used to kill job")
pbs_config.addOption('kill_res_pattern', '(^$)|(qdel: Unknown Job Id)',
                 "String pattern for replay from the kill command")

tempstr = '''
env = os.environ
jobnumid = env["PBS_JOBID"]
os.system("mkdir /tmp/%s/" %jobnumid)
os.chdir("/tmp/%s/" %jobnumid)
os.environ["PATH"]+=":."
'''
pbs_config.addOption('preexecute', tempstr,
                 "String contains commands executing before submiting job to queue")

tempstr = '''
env = os.environ
jobnumid = env["PBS_JOBID"]
os.chdir("/tmp/")
os.system("rm -rf /tmp/%s/" %jobnumid)
'''
pbs_config.addOption('postexecute', tempstr,
                 "String contains commands executing before submiting job to queue")
pbs_config.addOption('jobnameopt', 'N', "String contains option name for name of job in batch system")
pbs_config.addOption('timeout', 600,
                 'Timeout in seconds after which a job is declared killed if it has not touched its heartbeat file. Heartbeat is touched every 30s so do not set this below 120 or so.')

# ------------------------------------------------
# SGE
sge_config = makeConfig('SGE', 'internal SGE command line interface')

sge_config.addOption('shared_python_executable', False, "Shared PYTHON")

sge_config.addOption('jobid_name', 'JOB_ID', "Name of environment with ID of the job")
sge_config.addOption('queue_name', 'QUEUE', "Name of environment with queue name of the job")
sge_config.addOption('heartbeat_frequency', '30', "Heartbeat frequency config variable")

# the -V options means that all environment variables are transferred to
# the batch job (ie the same as the default behaviour on LSF at CERN)
sge_config.addOption('submit_str', 'cd %s; qsub -cwd -V %s %s %s %s',
                 "String used to submit job to queue")
sge_config.addOption('submit_res_pattern', 'Your job (?P<id>\d+) (.+)',
                 "String pattern for replay from the submit command")

sge_config.addOption('stdoutConfig', '-o %s/stdout', "String pattern for defining the stdout")
sge_config.addOption('stderrConfig', '-e %s/stderr', "String pattern for defining the stderr")

sge_config.addOption('kill_str', 'qdel %s', "String used to kill job")
sge_config.addOption('kill_res_pattern', '(has registered the job +\d+ +for deletion)|(denied: job +"\d+" +does not exist)',
                 "String pattern for replay from the kill command")

# From the SGE man page on qsub
#
#===========================
# Furthermore, Grid Engine sets additional variables into the job's
# environment, as listed below.
#:
#:
# TMPDIR
#   The absolute path to the job's temporary working directory.
#=============================


sge_config.addOption('preexecute', 'os.chdir(os.environ["TMPDIR"])\nos.environ["PATH"]+=":."',
                 "String contains commands executing before submiting job to queue")
sge_config.addOption('postexecute', '', "String contains commands executing before submiting job to queue")
sge_config.addOption('jobnameopt', 'N', "String contains option name for name of job in batch system")
sge_config.addOption('timeout', 600, 'Timeout in seconds after which a job is declared killed if it has not touched its heartbeat file. Heartbeat is touched every 30s so do not set this below 120 or so.')

# ------------------------------------------------
# Mergers
merge_config = makeConfig('Mergers', 'parameters for mergers')
merge_config.addOption('associate', {'log': 'TextMerger', 'root': 'RootMerger',
                               'text': 'TextMerger', 'txt': 'TextMerger'}, 'Dictionary of file associations')
gangadir = getConfig('Configuration')['gangadir']
merge_config.addOption('merge_output_dir', gangadir +
                 '/merge_results', "location of the merger's outputdir")
merge_config.addOption('std_merge', 'TextMerger', 'Standard (default) merger')

# ------------------------------------------------
# Preparable
preparable_config = makeConfig('Preparable', 'Parameters for preparable applications')
preparable_config.addOption('unprepare_on_copy', False, 'Unprepare a prepared application when it is copied')

# ------------------------------------------------
# GPIComponentFilters
gpicomp_config = makeConfig('GPIComponentFilters', """Customization of GPI component object assignment
for each category there may be multiple filters registered, the one used being defined
in the configuration file in [GPIComponentFilters]
e.g: {'datasets':{'lhcbdatasets':lhcbFilter, 'testdatasets':testFilter}...}
""", is_open=False)

# ------------------------------------------------
# Output
output_config = makeConfig("Output", "configuration section for postprocessing the output")
output_config.addOption('AutoRemoveFilesWithJob', False,
                       'if True, each outputfile of type in list AutoRemoveFileTypes will be removed when the job is')
output_config.addOption('AutoRemoveFileTypes', [
                       'DiracFile'], 'List of outputfile types that will be auto removed when job is removed if AutoRemoveFilesWithJob is True')

output_config.addOption('PostProcessLocationsFileName', '__postprocesslocations__',
                       'name of the file that will contain the locations of the uploaded from the WN files')

output_config.addOption('FailJobIfNoOutputMatched', True,
                       'if True, a job will be marked failed if output is asked for but not found.')

output_config.addOption('ForbidLegacyOutput', True, 'if True, writing to the job outputdata and outputsandbox fields will be forbidden')

output_config.addOption('ForbidLegacyInput', True, 'if True, writing to the job inputsandbox field will be forbidden')

docstr_Ext = 'fileExtensions:list of output files that will be written to %s,\
backendPostprocess:defines where postprocessing should be done (WN/client) on different backends,\
uploadOptions:config values needed for the actual %s upload'

# LocalFile
#    LocalPost = {'Localhost': 'WN', 'Interactive': 'WN', 'CREAM': 'client', 'Dirac': 'client'}
#
#    LocalUpOpt = {}
#
#    LocalFileExt = docstr_Ext % ('Local', 'Local')
#
#    outputconfig.addOption('LocalFile',
#                            {'fileExtensions': ['*.txt'],
#                             'backendPostprocess' : LocalPost,
#                             'uploadOptions' : LocalUpOpt},
#                            LocalFileExt)


# LCGSEFILE

LCGSEBakPost = {'LSF': 'client', 'PBS': 'client', 'LCG': 'WN', 'CREAM': 'WN',
                'ARC': 'WN', 'Localhost': 'WN', 'Interactive': 'WN'}
LCGSEUpOpt = {'LFC_HOST': 'lfc-dteam.cern.ch', 'dest_SRM': 'srm-public.cern.ch'}
LCGSEFileExt = docstr_Ext % ('LCG SE', 'LCG')

output_config.addOption('LCGSEFile',
                       {'fileExtensions': ['*.root', '*.asd'],
                        'backendPostprocess': LCGSEBakPost,
                        'uploadOptions': LCGSEUpOpt},
                       LCGSEFileExt)

# DiracFile
## TODO MOVE ME TO GANGADIRAC!!!
# Should this be in Core or elsewhere?
diracBackPost = {'Dirac': 'submit', 'LSF': 'WN', 'PBS': 'WN', 'LCG': 'WN',
                 'CREAM': 'WN', 'ARC': 'WN', 'Localhost': 'WN', 'Interactive': 'WN'}
diracFileExts = docstr_Ext % ('DIRAC', 'DIRAC')

output_config.addOption('DiracFile',
                       {'fileExtensions': ['*.dst'],
                        'backendPostprocess': diracBackPost,
                        'uploadOptions': {},
                        'defaultSite': {'upload': 'CERN-USER', 'download': 'CERN-USER'}},
                       diracFileExts)

# GoogleFile

GoogleFileBackPost = {'Dirac': 'client', 'LSF': 'client', 'PBS': 'client', 'LCG': 'client',
                      'CREAM': 'client', 'ARC': 'client', 'Localhost': 'client', 'Interactive': 'client'}
GoogleFileExts = docstr_Ext % ('GoogleDrive', 'Google')

output_config.addOption('GoogleFile',
                       {'fileExtensions': [],
                        'backendPostprocess': GoogleFileBackPost,
                        'uploadOptions': {}},
                       GoogleFileExts)

# MassStorageFile

import pwd
import grp
Conf_config = getConfig('Configuration')
if 'user' in Conf_config:
    user = Conf_config ['user']
else:
    #import sys
    #sys.stderr.write('Configure Error: %s' % str(err) )
    import getpass
    user = getpass.getuser()


## FIXME Sometimes the wrong user is set gere for the unittests, I've added this to correct for it - rcurrie
try:
    pwd_nam = pwd.getpwnam(user)
except:
    import getpass
    user = getpass.getuser()
    pwd_nam = pwd.getpwnam(user)

groupid = grp.getgrgid(pwd_nam.pw_gid).gr_name
groupnames = {'z5': 'lhcb', 'zp': 'atlas', 'zh': 'cms', 'vl': 'na62'}
groupname = groupnames.get(groupid, 'undefined')

try:
    import os.path
    massStoragePath = os.path.join(os.environ['EOS_HOME'], 'ganga')
except KeyError:
    massStoragePath = "/eos/%s/user/%s/%s/ganga" % (
        groupname, user[0], user)

# From:
# http://eos.cern.ch/index.php?option=com_content&view=article&id=87:using-eos-at-cern&catid=31:general&Itemid=41
protoByExperiment = {'atlas': 'root://eosatlas.cern.ch',
                     'cms': 'root://eocms.cern.ch',
                     'lhcb': 'root://eoslhcb.cern.ch',
                     'alice': 'root://eosalice.cern.ch',
                     # These last 2 are guesses based on the standard
                     'na62': 'root://eosna62.cern.ch',
                     'undefined': 'root://eos.cern.ch'}
defaultMassStorageProto = protoByExperiment[groupname]

prefix = '/afs/cern.ch/project/eos/installation/%s/bin/eos.select ' % groupname
massStorageUploadOptions = {'mkdir_cmd': prefix + 'mkdir', 'cp_cmd':
                            prefix + 'cp', 'ls_cmd': prefix + 'ls', 'path': massStoragePath}

massStorageFileExt = docstr_Ext % ('Mass Storage', 'EOS')

massStorageBackendPost = {'LSF': 'WN', 'PBS': 'WN', 'LCG': 'client', 'CREAM': 'client',
                          'ARC': 'client', 'Localhost': 'WN', 'Interactive': 'client', 'Dirac': 'client'}

output_config.addOption('MassStorageFile',
                       {'fileExtensions': [''],
                        'backendPostprocess': massStorageBackendPost,
                        'uploadOptions': massStorageUploadOptions,
                        'defaultProtocol': defaultMassStorageProto},
                       massStorageFileExt)

# ------------------------------------------------
# Display
disp_config = makeConfig('Display', 'control the printing style of the different registries ("jobs","box","tasks"...)')
disp_config.addOption('config_name_colour', 'fx.bold',
                         'colour print of the names of configuration sections and options')
disp_config.addOption(
    'config_docstring_colour', 'fg.green', 'colour print of the docstrings and examples')
disp_config.addOption(
    'config_value_colour', 'fx.bold', 'colour print of the configuration values')
disp_config.addOption('jobs_columns',
                 ("fqid", "status", "name", "subjobs", "application",
                  "backend", "backend.actualCE", "comment"),
                 'list of job attributes to be printed in separate columns')

disp_config.addOption('jobs_columns_width',
                 {'fqid': 8, 'status': 10, 'name': 10, 'subjobs': 8, 'application':
                     15, 'backend': 15, 'backend.actualCE': 45, 'comment': 30},
                 'width of each column')

disp_config.addOption('jobs_columns_functions',
                 {'subjobs': "lambda j: len(j.subjobs)", 'application': "lambda j: j.application.__class__.__name__",
                  'backend': "lambda j:j.backend.__class__.__name__", 'comment': "lambda j: j.comment"},
                 'optional converter functions')

disp_config.addOption('jobs_columns_show_empty',
                 ['fqid'],
                 'with exception of columns mentioned here, hide all values which evaluate to logical false (so 0,"",[],...)')

disp_config.addOption('jobs_status_colours',
                 {'new': 'fx.normal',
                  'submitted': 'fg.orange',
                  'running': 'fg.green',
                  'completed': 'fg.blue',
                  'failed': 'fg.red'
                  },
                 'colours for jobs status')

# add display default values for the box
disp_config.addOption('box_columns',
                 ("id", "type", "name", "application"),
                 'list of job attributes to be printed in separate columns')

disp_config.addOption('box_columns_width',
                 {'id': 5, 'type': 20, 'name': 40, 'application': 15},
                 'width of each column')

disp_config.addOption('box_columns_functions',
                 {'application': "lambda obj: obj.application._name"},
                 'optional converter functions')

disp_config.addOption('box_columns_show_empty',
                 ['id'],
                 'with exception of columns mentioned here, hide all values which evaluate to logical false (so 0,"",[],...)')

# display default values for task list
markup = ANSIMarkup()
str_done = markup("done", overview_colours["completed"])
disp_config.addOption('tasks_columns',
                     ("id", "Type", "Name", "State",
                      "Comment", "Jobs", str_done),
                     'list of job attributes to be printed in separate columns')

disp_config.addOption('tasks_columns_width',
                     {"id": 5, "Type": 13, "Name": 22, "State": 9,
                         "Comment": 30, "Jobs": 33, str_done: 5},
                     'width of each column')

disp_config.addOption('tasks_columns_functions',
                     {'Name': "lambda t : t.name",
                      'Type': "lambda task : task._name",
                      'State ': "lambda task : task.status",
                      'Comment ': "lambda task : task.comment",
                      'Jobs': "lambda task : task.n_all()",
                      str_done: "lambda task : task.n_status('completed')",
                      },
                     'optional converter functions')

disp_config.addOption('tasks_columns_show_empty',
                     ['id', 'Jobs',
                         str_done],
                     'with exception of columns mentioned here, hide all values which evaluate to logical false (so 0,"",[],...)')

disp_config.addOption(
    'tasks_show_help', True, 'change this to False if you do not want to see the help screen if you first type "tasks" in a session')

# ------------------------------------------------
# Tasks
tasks_config = makeConfig('Tasks', 'Tasks configuration options')
tasks_config.addOption('TaskLoopFrequency', 60., "Frequency of Task Monitoring loop in seconds")
tasks_config.addOption('ForceTaskMonitoring', False, "Monitor tasks even if the monitoring loop isn't enabled")
tasks_config.addOption('disableTaskMon', False, "Should I disable the Task Monitoring loop?")

# ------------------------------------------------
# MonitoringServices
mon_config = makeConfig('MonitoringServices', """External monitoring systems are used
to follow the submission and execution of jobs. Each entry in this section
defines a monitoring plugin used for a particular combination of application
and backend. Asterisks may be used to specify any application or any
backend. The configuration entry syntax:

ApplicationName/BackendName = dot.path.to.monitoring.plugin.class.

Example: DummyMS plugin will be used to track executables run on all backends:

Executable/* = Ganga.Lib.MonitoringServices.DummyMS.DummyMS

""", is_open=True)

# ------------------------------------------------
# Registry Dirty Monitoring Services (not related to actual Job Monitoring)
reg_config = makeConfig('Registry','')
reg_config.addOption('AutoFlusherWaitTime', 30, 'Time to wait between auto-flusher runs')
