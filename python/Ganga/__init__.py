# System Imports
import os
import re
import inspect

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
_gangaVersion = '$Name: 6.1.13 $'

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
    import Ganga.Utility.util
    hostname = Ganga.Utility.util.hostname()
except Exception as x:  # fixme: use OSError instead?
    hostname = 'localhost'

# ------------------------------------------------
# Setup all configs for this module
from Ganga.Utility.Config import makeConfig, expandvars

# ------------------------------------------------
# System
# the system variables (such as VERSION) are put to DEFAULTS section of the config module
# so you can refer to them in the config file
# additionally they will be visible in the (write protected) [System]
# config module
syscfg = makeConfig('System', "parameters of this ganga session (read-only)", cfile=False)
syscfg.addOption('GANGA_VERSION', _gangaVersion, '')
syscfg.addOption('GANGA_PYTHONPATH', _gangaPythonPath, 'location of the ganga core packages')
syscfg.addOption('GANGA_CONFIG_PATH', '','site/group specific configuration files as specified by --config-path or GANGA_CONFIG_PATH variable')
syscfg.addOption('GANGA_CONFIG_FILE', '', 'current user config file used')
syscfg.addOption('GANGA_HOSTNAME', hostname, 'local hostname where ganga is running')

# ------------------------------------------------
# Configuration
# the SCRIPTS_PATH must be initialized before the config files are loaded
# for the path to be correctly prepended
config = makeConfig("Configuration", "global configuration parameters.\nthis is a catch all section.")
config.addOption('SCRIPTS_PATH', 'Ganga/scripts', """the search path to scripts directory.
When running a script from the system shell (e.g. ganga script) this path is used to search for script""")

config.addOption('LOAD_PATH', '', "the search path for the load() function")
config.addOption('RUNTIME_PATH', '',
"""path to runtime plugin packages where custom handlers may be added.
Normally you should not worry about it.
If an element of the path is just a name (like in the example below)
then the plugins will be loaded using current python path. This means that
some packages such as GangaTest may be taken from the release area.""",
        examples="RUNTIME_PATH = GangaGUIRUNTIME_PATH = /my/SpecialExtensions:GangaTest ")

config.addOption('TextShell', 'IPython', """ The type of the interactive shell: IPython (cooler) or Console (limited)""")
config.addOption('StartupGPI', '', 'block of GPI commands executed at startup')
config.addOption('ReleaseNotes', True, 'Flag to print out the relevent subsection of release notes for each experiment at start up')
config.addOption('gangadir', expandvars(None, '~/gangadir'),
                 'Location of local job repositories and workspaces. Default is ~/gangadir but in somecases (such as LSF CNAF) this needs to be modified to point to the shared file system directory.', filter=Ganga.Utility.Config.expandvars)
config.addOption(
    'repositorytype', 'LocalXML', 'Type of the repository.', examples='LocalXML')
config.addOption('workspacetype', 'LocalFilesystem',
                 'Type of workspace. Workspace is a place where input and output sandbox of jobs are stored. Currently the only supported type is LocalFilesystem.')

config.addOption('user', '',
    'User name. The same person may have different roles (user names) and still use the same gangadir. Unless explicitly set this option defaults to the real user name.')
config.addOption('resubmitOnlyFailedSubjobs', True,
                 'If TRUE (default), calling job.resubmit() will only resubmit FAILED subjobs. Note that the auto_resubmit mechanism will only ever resubmit FAILED subjobs.')
config.addOption('SMTPHost', 'localhost', 'The SMTP server for notification emails to be sent, default is localhost')
config.addOption('deleteUnusedShareDir', 'always',
                 'If set to ask the user is presented with a prompt asking whether Shared directories not associated with a persisted Ganga object should be deleted upon Ganga exit. If set to never, shared directories will not be deleted upon exit, even if they are not associated with a persisted Ganga object. If set to always (the default), then shared directories will always be deleted if not associated with a persisted Ganga object.')

config.addOption('autoGenerateJobWorkspace', False, 'Autogenerate workspace dirs for new jobs')

# add named template options
config.addOption('namedTemplates_ext', 'tpl',
                 'The default file extension for the named template system. If a package sets up their own by calling "establishNamedTemplates" from python/Ganga/GPIDev/Lib/Job/NamedJobTemplate.py in their ini file then they can override this without needing the config option')
config.addOption('namedTemplates_pickle', False,
                 'Determines if named template system stores templates in pickle file format (True) or in the Ganga streamed object format (False). By default streamed object format which is human readable is used. If a package sets up their own by calling "establishNamedTemplates" from python/Ganga/GPIDev/Lib/Job/NamedJobTemplate.py in their ini file then they can override this without needing the config option')

# add server options
config.addOption('ServerPort', 434343, 'Port for the Ganga server to listen on')
config.addOption('ServerTimeout', 60, 'Timeout in minutes for auto-server shutdown')
config.addOption('ServerUserScript', "", "Full path to user script to call periodically. The script will be executed as if called within Ganga by 'execfile'.")
config.addOption('ServerUserScriptWaitTime', 300, "Time in seconds between executions of the user script")

config.addOption('confirm_exit', 1, 'Ask the user on exit if we should exit, (this is passed along to IPython)')
config.addOption('force_start', False, 'Ignore disk checking on startup')

# ------------------------------------------------
# IPython
ipconfig = makeConfig('TextShell_IPython', '''IPython shell configuration
See IPython manual for more details:
http://ipython.scipy.org/doc/manual''')
ipconfig.addOption('args', "['-colors','LightBG', '-autocall','0', '-pprint']", 'Options to be passed to ipython for initialization')

# ------------------------------------------------
# Shell
makeConfig("Shell", "configuration parameters for internal Shell utility.")

# ------------------------------------------------
# Shell
queuesconfig = makeConfig("Queues", "configuration section for the queues")
queuesconfig.addOption('Timeout', None, 'default timeout for queue generated processes')
queuesconfig.addOption('NumWorkerThreads', 3, 'default number of worker threads in the queues system')

# ------------------------------------------------
# MSGMS
monConfig = makeConfig('MSGMS', 'Settings for the MSGMS monitoring plugin. Cannot be changed ruding the interactive Ganga session.')
monConfig.addOption('server', 'dashb-mb.cern.ch', 'The server to connect to')
monConfig.addOption('port', 61113, 'The port to connect to')
monConfig.addOption('username', 'ganga', '')
monConfig.addOption('password', 'analysis', '')
monConfig.addOption('message_destination', '/queue/ganga.status', '')
monConfig.addOption('usage_message_destination', "/queue/ganga.usage", '')
monConfig.addOption('job_submission_message_destination', "/queue/ganga.jobsubmission", '')

# ------------------------------------------------
# Display
import Ganga.Utility.ColourText
makeConfig('Display', """control the content and appearence of printing ganga objects: attributes,colours,etc.
If ANSI text colours are enabled, then individual colours may be specified like this:
fg.xxx - Foreground: %s
bg.xxx - Background: %s
fx.xxx - Effects: %s
""" % (Ganga.Utility.ColourText.Foreground.__doc__, Ganga.Utility.ColourText.Background.__doc__, Ganga.Utility.ColourText.Effects.__doc__))

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
gpiconfig = makeConfig('GPI_Semantics',
                       'Customization of GPI behaviour. These options may affect the semantics of the Ganga GPI interface (what may result in a different behaviour of scripts and commands).')
gpiconfig.addOption('job_submit_keep_going', False,
                    'Keep on submitting as many subjobs as possible. Option to j.submit(), see Job class for details')
gpiconfig.addOption('job_submit_keep_on_fail', False,
                    'Do not revert job to new status even if submission failed. Option to j.submit(), see Job class for details')

# ------------------------------------------------
# Logging
logconfig = makeConfig("Logging", """control the messages printed by Ganga
The settings are applied hierarchically to the loggers. Ganga is the name of the top-level logger which
applies by default to all Ganga.* packages unless overriden in sub-packages.
You may define new loggers in this section.
The log level may be one of: CRITICAL ERROR WARNING INFO DEBUG
""", is_open=True)

# FIXME: Ganga WARNING should be turned into INFO level when the messages
# are reviewed in all the code
logconfig.addOption('Ganga', "INFO", "top-level logger")
logconfig.addOption('Ganga.Runtime.bootstrap', "INFO", 'FIXME')
logconfig.addOption('Ganga.GPIDev', "INFO", "logger of Ganga.GPIDev.* packages")
logconfig.addOption('Ganga.Utility.logging', "WARNING", "logger of the Ganga logging package itself (use with care!)")
logconfig.addOption('_format', "NORMAL", "format of logging messages: TERSE,NORMAL,VERBOSE,DEBUG")
logconfig.addOption('_colour', True, "enable ASCII colour formatting of messages e.g. errors in red")
logconfig.addOption('_logfile', "~/.ganga.log", "location of the logfile")
logconfig.addOption('_logfile_size', 100000,
                 "the size of the logfile (in bytes), the rotating log will never exceed this file size")  # 100 K
logconfig.addOption('_interactive_cache', True,
                 'if True then the cache used for interactive sessions, False disables caching')
logconfig.addOption('_customFormat', "", "custom formatting string for Ganga logging\n e.g. '%(name)-35s: %(levelname)-8s %(message)s'")

# ------------------------------------------------
# PollThread
config = makeConfig('PollThread', 'background job status monitoring and output retrieval')
config.addOption('repeat_messages', False, 'if 0 then log only once the errors for a given backend and do not repeat them anymore')
config.addOption('autostart', True, 'enable monitoring automatically at startup, in script mode monitoring is disabled by default, in interactive mode it is enabled', type=type(True))  # enable monitoring on startup
config.addOption('autostart_monThreads', True, 'enable populating of the monitoring worker threads')
config.addOption('base_poll_rate', 2, 'internal supervising thread', hidden=1)
config.addOption('MaxNumResubmits', 5, 'Maximum number of automatic job resubmits to do before giving')
config.addOption('MaxFracForResubmit', 0.25, 'Maximum fraction of failed jobs before stopping automatic resubmission')
config.addOption('update_thread_pool_size', 5, 'Size of the thread pool. Each threads monitors a specific backaend at a given time. Minimum value is one, preferably set to the number_of_backends + 1')
config.addOption('default_backend_poll_rate', 30, 'Default rate for polling job status in the thread pool. This is the default value for all backends.')
config.addOption('Local', 10, 'Poll rate for Local backend.')
config.addOption('LCG', 30, 'Poll rate for LCG backend.')
config.addOption('Condor', 30, 'Poll rate for Condor backend.')
config.addOption('gLite', 30, 'Poll rate for gLite backend.')
config.addOption('LSF', 20, 'Poll rate for LSF backend.')
config.addOption('PBS', 20, 'Poll rate for PBS backend.')
config.addOption('Dirac', 50, 'Poll rate for Dirac backend.')
config.addOption('Panda', 50, 'Poll rate for Panda backend.')

# Note: the rate of this callback is actually
# MAX(base_poll_rate,callbacks_poll_rate)
config.addOption('creds_poll_rate', 30, "The frequency in seconds for credentials checker")
config.addOption('diskspace_poll_rate', 30, "The frequency in seconds for free disk checker")
config.addOption('DiskSpaceChecker', "", "disk space checking callback. This function should return False when there is no disk space available, True otherwise")
config.addOption('max_shutdown_retries', 5, 'OBSOLETE: this option has no effect anymore')
config.addOption('numParallelJobs', 5, 'Number of Jobs to update the status for in parallel')

# ------------------------------------------------
# Feedback
config = makeConfig('Feedback', 'Settings for the Feedback plugin. Cannot be changed ruding the interactive Ganga session.')
config.addOption('uploadServer', 'http://gangamon.cern.ch/django/errorreports', 'The server to connect to')

# ------------------------------------------------
# Associations
config = makeConfig(
    "File_Associations", 'Default associations between file types and file-viewing commands. The name identifies the extension and the value the commans. New extensions can be added. A single & after the command indicates that the process will be started in the background. A && after the command indicates that a new terminal will be opened and the command executed in that terminal.', is_open=True)

config.addOption("newterm_command", "xterm",
                 'Command for opening a new terminal (xterm, gnome-terminal, ...')
config.addOption("newterm_exeopt", "-e",
                 'Option to give to a new terminal to tell it to execute a command.')
config.addOption(
    "listing_command", "ls -ltr", 'Command for listing the content of a directory')
config.addOption('fallback_command', 'less',
                 'Default command to use if there is no association with the file type')
config.addOption('htm', 'firefox &', 'Command for viewing html files.')
config.addOption('html', 'firefox &', 'Command for viewing html files.')
config.addOption('root', 'root.exe &&', 'Command for opening ROOT files.')
config.addOption('tar', 'file-roller &', 'Command for opening tar files.')
config.addOption('tgz', 'file-roller &', 'Command for opening tar files.')

# ------------------------------------------------
# Root
config = makeConfig('ROOT', "Options for Root backend")
## Not needed when we can't do option substitution internally but support it at the .gangarc level!!!!! 27-09-2015 rcurrie
#config.addOption('lcgpath', getLCGRootPath(), 'Path of the LCG release that the ROOT project and it\'s externals are taken from')
config.addOption('arch', 'x86_64-slc6-gcc48-opt', 'Architecture of ROOT')
## Auto-Interporatation doesn't appear to work when setting the default value
#config.addOption('location', '${lcgpath}/ROOT/${version}/${arch}/', 'Location of ROOT')
config.addOption('location', '%s/ROOT/6.04.02/x86_64-slc6-gcc48-opt' % getLCGRootPath(), 'Location of ROOT')
config.addOption('path', '', 'Set to a specific ROOT version. Will override other options.')
## Doesn't appear to work see above ^^^
#config.addOption('pythonhome', '${lcgpath}/Python/${pythonversion}/${arch}/','Location of the python used for execution of PyROOT script')
config.addOption('pythonhome', '%s/Python/2.7.9.p1/x86_64-slc6-gcc48-opt' % getLCGRootPath(), 'Location of the python used for execution of PyROOT script')
config.addOption('pythonversion', '2.7.9.p1', "Version number of python used for execution python ROOT script")
config.addOption('version', '6.04.02', 'Version of ROOT')

# ------------------------------------------------
# Local
config = makeConfig('Local', 'parameters of the local backend (jobs in the background on localhost)')
config.addOption('remove_workdir', True, 'remove automatically the local working directory when the job completed')
config.addOption('location', None, 'The location where the workdir will be created. If None it defaults to the value of $TMPDIR')

# ------------------------------------------------
# LCG
config = makeConfig('LCG', 'LCG/gLite/EGEE configuration parameters')
#gproxy_config = getConfig('GridProxy_Properties')

# set default values for the configuration parameters
config.addOption(
    'EDG_ENABLE', False, 'enables/disables the support of the EDG middleware')

config.addOption('EDG_SETUP', '/afs/cern.ch/sw/ganga/install/config/grid_env_auto.sh',
                 'sets the LCG-UI environment setup script for the EDG middleware',
                 filter=Ganga.Utility.Config.expandvars)

config.addOption(
    'GLITE_ENABLE', True, 'Enables/disables the support of the GLITE middleware')

config.addOption('GLITE_SETUP', '/afs/cern.ch/sw/ganga/install/config/grid_env_auto.sh',
                 'sets the LCG-UI environment setup script for the GLITE middleware',
                 filter=Ganga.Utility.Config.expandvars)

config.addOption('VirtualOrganisation', 'dteam',
                 'sets the name of the grid virtual organisation')

config.addOption('ConfigVO', '', 'sets the VO-specific LCG-UI configuration script for the EDG resource broker',
                 filter=Ganga.Utility.Config.expandvars)

config.addOption('Config', '', 'sets the generic LCG-UI configuration script for the GLITE workload management system',
                 filter=Ganga.Utility.Config.expandvars)

config.addOption(
    'AllowedCEs', '', 'sets allowed computing elements by a regular expression')
config.addOption(
    'ExcludedCEs', '', 'sets excluded computing elements by a regular expression')

config.addOption(
    'GLITE_WMS_WMPROXY_ENDPOINT', '', 'sets the WMProxy service to be contacted')
config.addOption('GLITE_ALLOWED_WMS_LIST', [], '')

config.addOption('MyProxyServer', 'myproxy.cern.ch', 'sets the myproxy server')
config.addOption('RetryCount', 3, 'sets maximum number of job retry')
config.addOption(
    'ShallowRetryCount', 10, 'sets maximum number of job shallow retry')

config.addOption(
    'Rank', '', 'sets the ranking rule for picking up computing element')
config.addOption('ReplicaCatalog', '', 'sets the replica catalogue server')
config.addOption('StorageIndex', '', 'sets the storage index')

config.addOption(
    'DefaultSE', 'srm.cern.ch', 'sets the default storage element')
config.addOption('DefaultSRMToken', '',
                 'sets the space token for storing temporary files (e.g. oversized input sandbox)')
config.addOption(
    'DefaultLFC', 'prod-lfc-shared-central.cern.ch', 'sets the file catalogue server')
config.addOption('BoundSandboxLimit', 10 * 1024 * 1024,
                 'sets the size limitation of the input sandbox, oversized input sandbox will be pre-uploaded to the storage element specified by \'DefaultSE\' in the area specified by \'DefaultSRMToken\'')

config.addOption('Requirements', 'Ganga.Lib.LCG.LCGRequirements',
                 'sets the full qualified class name for other specific LCG job requirements')

config.addOption('SandboxCache', 'Ganga.Lib.LCG.LCGSandboxCache',
                 'sets the full qualified class name for handling the oversized input sandbox')

config.addOption('GliteBulkJobSize', 50,
                 'sets the maximum number of nodes (i.e. subjobs) in a gLite bulk job')

config.addOption('SubmissionThread', 10,
                 'sets the number of concurrent threads for job submission to gLite WMS')

config.addOption(
    'SubmissionTimeout', 300, 'sets the gLite job submission timeout in seconds')

config.addOption('StatusPollingTimeout', 300,
                 'sets the gLite job status polling timeout in seconds')

config.addOption('OutputDownloaderThread', 10,
                 'sets the number of concurrent threads for downloading job\'s output sandbox from gLite WMS')

config.addOption('SandboxTransferTimeout', 60,
                 'sets the transfer timeout of the oversized input sandbox')

config.addOption(
    'JobLogHandler', 'WMS', 'sets the way the job\'s stdout/err are being handled.')

config.addOption('MatchBeforeSubmit', False,
                 'sets to True will do resource matching before submitting jobs, jobs without any matched resources will fail the submission')

config.addOption('IgnoreGliteScriptHeader', False,
                 'sets to True will load script-based glite-wms-* commands forcely with current python, a trick for 32/64 bit compatibility issues.')

# ------------------------------------------------
# GridSimulator
config = makeConfig('GridSimulator', 'Grid Simulator configuration parameters')

config.addOption('submit_time', 'random.uniform(1,10)',
                 'python expression which returns the time it takes (in seconds) to complete the Grid.submit() command (also for subjob in bulk emulation)')
config.addOption(
    'submit_failure_rate', 0.0, 'probability that the Grid.submit() method fails')

config.addOption('cancel_time', 'random.uniform(1,5)',
                 'python expression which returns the time it takes (in seconds) to complete the Grid.cancel() command (also for subjob in bulk emulation)')
config.addOption(
    'cancel_failure_rate', 0.0, 'probability that the Grid.cancel() method fails')

config.addOption('status_time', 'random.uniform(1,5)',
                 'python expression which returns the time it takes (in seconds) to complete the status command (also for subjob in bulk emulation)')

config.addOption('get_output_time', 'random.uniform(1,5)',
                 'python expression which returns the time it takes (in seconds) to complete the get_output command (also for subjob in bulk emulation)')

#config.addOption('bulk_submit_time','random.uniform(1,2)','python expression which returns the time it takes (in seconds) to complete the submission of a single job within the Grid.native_master_submit() command')
#config.addOption('bulk_submit_failure_rate',0.0,'probabilty that the Grid.native_master_submit() fails')

#config.addOption('bulk_cancel_time','random.uniform(1,2)','python expression which returns the time it takes (in seconds) to complete the cancellation of a single job within the Grid.native_master_cancel() command')
#config.addOption('bulk_cancel_failure_rate',0.0,'probabilty that the Grid.native_master_cancel() fails')

config.addOption('job_id_resolved_time', 'random.uniform(1,2)',
                 'python expression which returns the time it takes (in seconds) to complete the resolution of all the id of a subjob (when submitted in bulk) this is the time the NODE_ID becomes available from the monitoring)')

#config.addOption('job_scheduled_time','random.uniform(10,20)', 'python expression which returns the time the job stays in the scheduled state')
#config.addOption('job_running_time','random.uniform(10,20)', 'python expression which returns the time the job stays in the running state')
config.addOption('job_finish_time', 'random.uniform(10,20)',
                 'python expression which returns the time when the job enters the Done success or Failed state')
config.addOption(
    'job_failure_rate', 0.0, 'probability of the job to enter the Failed state')
