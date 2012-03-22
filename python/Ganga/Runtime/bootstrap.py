################################################################################
# Ganga - a computational task management tool for easy access to Grid resources
# http://cern.ch/ganga
#
# Copyright (C) 2003 The Ganga Project
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
#
################################################################################

# store Ganga version based on CVS sticky tag for this file
_gangaVersion = "$Name: Ganga-SVN $"

import re
# [N] in the pattern is important because it prevents CVS from expanding the pattern itself!
r = re.compile(r'\$[N]ame: (?P<version>\S+) \$').match(_gangaVersion)
if r:
   _gangaVersion = r.group('version')
else:
   _gangaVersion = "SVN_TRUNK"

# store a path to Ganga libraries

import os.path, Ganga
_gangaPythonPath = os.path.dirname(os.path.dirname(Ganga.__file__))

from Ganga.Utility.files import fullpath

import sys,time

#import atexit, traceback
#def register(f):
#   print '*'*10
#   print 'register',f
#   traceback.print_stack()
#   _register(f)
#_register = atexit.register
#atexit.register = register


class GangaProgram:
    """ High level API to create instances of Ganga programs and configure/run it """

    def __init__(self,hello_string=None,argv=sys.argv):
        """ make an instance of Ganga program
        use default hello_string if not specified
        use sys.argv as arguments if not specified"""

        self.argv = argv[:]
        
        #record the start time.Currently we are using this in performance measurements 
        # see Ganga/test/Performance tests
        self.start_time = time.time()

        if hello_string is None:
            self.hello_string = """
*** Welcome to Ganga ***
Version: %s
Documentation and support: http://cern.ch/ganga
Type help() or help('index') for online help.

This is free software (GPL), and you are welcome to redistribute it
under certain conditions; type license() for details.

""" % _gangaVersion
        else:
            self.hello_string = hello_string

        # by default enter interactive mode
        self.interactive = True

        import os.path
        self.default_config_file = os.path.expanduser('~/.gangarc')

        # this is a TEMPORARY hack to enable some GUI-specific parts of the core such as monitoring
        # replaced by self.options.GUI
        #self.gui_enabled_hack = False

    def exit(self,*msg):
       print >> sys.stderr, self.hello_string
       for m in msg:
          print >> sys.stderr,'ganga:',m
       sys.exit(1)

    # parse the options
    
    def parseOptions(self):
        from optparse import OptionParser

        usage = self.hello_string+"""\nusage: %prog [options] [script] [args] ..."""

        parser = OptionParser(usage,version=_gangaVersion)

        parser.add_option("-i", dest="force_interactive", action="store_true",
                          help='enter interactive mode after running script')

        parser.add_option("--webgui", dest="webgui",  action="store_true", default='False',
                          help='starts web GUI monitoring server')

        parser.add_option('--gui',dest="GUI",action='store_true',default=False,help='Run Ganga in the GUI mode.')
                
        parser.add_option("--config", dest="config_file",action="store", metavar="FILE",
                          help='read user configuration from FILE, overrides the GANGA_CONFIG_FILE environment variable. Default: ~/.gangarc')
        
        parser.add_option("--config-path",dest='config_path',action="store", default=None,
                          help='site/experiment config customization path, overrides the GANGA_CONFIG_PATH environment variable. The relative paths are resolved wrt to the release directory. To use a specific file you should specify the absolute path. Default: None')

        parser.add_option("-g","--generate-config",dest='generate_config',action="store_const",const=1,
                          help='generate a default config file, backup the existing one')
        
        parser.add_option("-o","--option",dest='cmdline_options',action="append", default=[],metavar='EXPR',
                          help='set configuration options, may be repeated mutiple times,'
                               'for example: -o[Logging]Ganga.Lib=DEBUG -oGangaLHCb=INFO -o[Configuration]TextShell = IPython '
                               'The VALUE of *_PATH option is prepended. To reset it use :::VALUE')

        parser.add_option("--quiet", dest="force_loglevel",action="store_const",const='ERROR',
                          help='only ERROR messages are printed')
        
        parser.add_option("--very-quiet", dest="force_loglevel",action="store_const",const='CRITICAL',
                          help='only CRITICAL messages are printed')
        
        parser.add_option("--debug",dest="force_loglevel",action="store_const",const='DEBUG',
                          help='all messages including DEBUG are printed')
        
        parser.add_option("--no-mon",dest='monitoring',action="store_const",const=0,
                          help='disable the monitoring loop (useful if you run multiple Ganga sessions)')

        parser.add_option("--no-prompt",dest='prompt',action="store_const",const=0,
                          help='never prompt interactively for anything except IPython (FIXME: this is not fully implemented)')
        
        parser.add_option("--no-rexec", dest = "rexec", action="store_const",const=0,
                          help='rely on existing environment and do not re-exec ganga process'
                               'to setup runtime plugin modules (affects LD_LIBRARY_PATH)') 
        
        parser.add_option("--test",dest='TEST',action="store_true", default=False,
                          help='run Ganga test(s) using internal test-runner. It requires GangaTest package to be installed.'
                               'Usage example: *ganga --test Ganga/test/MyTestcase* .'
                               'Refer to [TestingFramework] section in Ganga config for more information on how to configure the test runner.')
        
        parser.set_defaults(force_interactive=False, config_file=None, force_loglevel=None,rexec=1, monitoring=1, prompt=1, generate_config=None)
        parser.disable_interspersed_args()

        (self.options, self.args) = parser.parse_args(args=self.argv[1:])

        def file_opens(f,message):
            try:
                return file(f)
            except IOError,x:
               self.exit(message,x)

        self.options.config_file_set_explicitly = not self.options.config_file is None

        # use GANGA_CONFIG_FILE env var if it's set
        if self.options.config_file is None:
           self.options.config_file = os.environ.get('GANGA_CONFIG_FILE',None)
           
        if self.options.config_file:
           import Ganga.Utility.files
           self.options.config_file = Ganga.Utility.files.expandfilename(self.options.config_file)
           file_opens(self.options.config_file,'reading configuration file')

        # we run in the batch mode if a script has been specified and other options (such as -i) do not force it
        if len(self.args) > 0:
            if not self.options.force_interactive:
                    self.interactive = False
# Can't check here if the file is readable, because the path isn't known
#           file_opens(self.args[0],'reading script')

    # this is an option method which runs an interactive wizard which helps new users to start with Ganga
    # the interactive mode is not entered if -c option was used
    def new_user_wizard(self):
        import os

        def generate(where):
            import shutil

            flavour = Ganga.Utility.Config.Config.getFlavour()
            print "Using flavour %s"%flavour
            if flavour:
                configtemplate = "CONFIG_TEMPLATE_%s.INI"%flavour   
            else:
                configtemplate = "CONFIG_TEMPLATE.INI"   
            shutil.copy(os.path.join(os.path.dirname(_gangaPythonPath),'templates',configtemplate),where)
            print >> sys.stderr, 'Created standard config file',where
            
        gangadir = os.path.expanduser('~/gangadir')
        if not os.path.exists(gangadir) \
           and not os.path.exists(self.default_config_file) \
           and not self.options.config_file_set_explicitly:

            if self.options.prompt:
                print >> sys.stderr, 'It seems that you run Ganga for the first time'
                print >> sys.stderr, 'Ganga will send a udp packet each time you start it in order to help the development team understand how ganga is used. You can disable this in the config file by resetting [Configuration]UsageMonitoringURL=  '
                if self.options.generate_config:
                    yes = 'Y'
                else:
                    yes = raw_input('Would you like to create config file ~/.gangarc with standard settings ([y]/n) ?')

                if yes == '' or yes[0:1].upper() == 'Y':
                    generate(self.default_config_file)
                    raw_input('Press <Enter> to continue.')

            os.mkdir(gangadir)

        else:

            # FIXME: store_backup is quite badly implemented
            def store_backup(f):
                if os.path.exists(f):
                    i = 0
                    for i in range(100):
                        bn = "%s.%.2d"%(f,i)
                        if not os.path.exists(bn):
                            os.rename(self.default_config_file,bn)
                            return bn
                    raise ValueError('too many backup files')

            if self.options.generate_config:
                try:
                    backup_name = store_backup(self.default_config_file)
                except Exception,x:
                   self.exit('Failed to create backup file %s'%backup_name)
                else:
                    print >> sys.stderr, 'Copied current config file to',backup_name
                generate(self.default_config_file)
                sys.exit(0) # FIXME: should not sys.exit()
                
    # configuration procedure: read the configuration files, configure and bootstrap logging subsystem
    def configure(self, logLevel = None ):
        import os,os.path

        import Ganga.Utility.Config 
        from Ganga.Utility.Config import ConfigError
        
        def parse_cmdline_config_options(cmdline_options):
           """ Parse a list of command line config options and return a list of triplets (section,option,value).
           In case of parsing errors, raise ConfigError exception.
           """
           import re
           mpat = re.compile(r'(\[(?P<section>\S+)\]|)(?P<option>[a-zA-z0-9._/]+)=(?P<value>.+)')
           section = None

           opts = []
           for o in cmdline_options:
              rpat = mpat.match(o)
              if rpat is None:
                 raise ConfigError('syntax error: "%s"'%o)
              else:
                 if rpat.group('section'):
                    section = rpat.group('section')
                 if section is None:
                    raise ConfigError('section not specified: %s' % o)
                 else:
                    opts.append((section,rpat.group('option'),rpat.group('value')))
           return opts


        def set_cmdline_config_options(sects=None):
           try:
              opts = parse_cmdline_config_options(self.options.cmdline_options)
              for section,option,val in opts:
                 should_set = True
                 if not sects is None and not section in sects:
                    should_set = False
                 if should_set:
                    config = Ganga.Utility.Config.setSessionValue(section,option,val)
           except ConfigError,x:
              self.exit('command line option error: %s'%str(x))

        # set logging options
        set_cmdline_config_options(sects=['Logging'])
        
        # we will be reexecutig the process so for the moment just shut up (unless DEBUG was forced with --debug)
        if self.options.rexec and not os.environ.has_key('GANGA_INTERNAL_PROCREEXEC') and not self.options.generate_config and not os.environ.has_key('GANGA_NEVER_REEXEC'):
            if self.options.force_loglevel != 'DEBUG':
               self.options.force_loglevel = 'CRITICAL'
            pass
        else: # say hello
            if logLevel: self.options.force_loglevel = logLevel
            if self.options.force_loglevel in (None,'DEBUG'):
                print >> sys.stderr, self.hello_string
#                self.new_user_wizard()

        if self.options.config_file is None:
            self.options.config_file = self.default_config_file
            
        # initialize logging for the initial phase of the bootstrap
        # will use the default, hardcoded log level in the module until pre-configuration procedure is complete
        import Ganga.Utility.logging

        Ganga.Utility.logging.force_global_level(self.options.force_loglevel)

        try:
           cf = file(self.options.config_file)
           first_line = cf.readline()
           import re
           r = re.compile(r'# Ganga configuration file \(\$[N]ame: (?P<version>\S+) \$\)').match(first_line)
           if not r:
              Ganga.Utility.logging.getLogger().error('file %s does not seem to be a Ganga config file',self.options.config_file)
              Ganga.Utility.logging.getLogger().error('try -g option to create valid ~/.gangarc')
           else:
               cv = r.group('version').split('-')
               if cv[1] == '4':
                   Ganga.Utility.logging.getLogger().error('file %s is old an Ganga 4 configuration file (%s)',self.options.config_file,r.group('version'))
                   Ganga.Utility.logging.getLogger().error('try -g option to create valid ~/.gangarc')
               else:
                   if cv[1] != '5':
                       Ganga.Utility.logging.getLogger().error('file %s was created by a development release (%s)',self.options.config_file, r.group('version'))
                       Ganga.Utility.logging.getLogger().error('try -g option to create valid ~/.gangarc')
        except IOError,x:
           pass # ignore all I/O errors (e.g. file does not exist), this is just an advisory check
              
        if self.options.config_path is None:
           try:
              self.options.config_path = os.environ['GANGA_CONFIG_PATH']
           except KeyError:
              self.options.config_path = ''

        import Ganga.Utility.files, Ganga.Utility.util
        self.options.config_path = Ganga.Utility.files.expandfilename(self.options.config_path)

        try:
           hostname = Ganga.Utility.util.hostname()
        except Exception,x: # fixme: use OSError instead?
           hostname = 'localhost'
        
        # the system variables (such as VERSION) are put to DEFAULTS section of the config module
        # so you can refer to them in the config file
        # additionally they will be visible in the (write protected) [System] config module
        syscfg = Ganga.Utility.Config.makeConfig('System',"parameters of this ganga session (read-only)",cfile=False)
        syscfg.addOption('GANGA_VERSION',_gangaVersion,'')
        syscfg.addOption('GANGA_PYTHONPATH',_gangaPythonPath,'location of the ganga core packages')
        syscfg.addOption('GANGA_CONFIG_PATH',self.options.config_path, 'site/group specific configuration files as specified by --config-path or GANGA_CONFIG_PATH variable')
        syscfg.addOption('GANGA_CONFIG_FILE',self.options.config_file,'current user config file used')
        syscfg.addOption('GANGA_HOSTNAME',hostname,'local hostname where ganga is running')
        
        def deny_modification(name,x):
           raise Ganga.Utility.Config.ConfigError('Cannot modify [System] settings (attempted %s=%s)'%(name,x))
        syscfg.attachUserHandler(deny_modification,None)
        syscfg.attachSessionHandler(deny_modification,None)        
       
        import Ganga.Utility.Config

        # the SCRIPTS_PATH must be initialized before the config files are loaded
        # for the path to be correctly prepended

        from Ganga.Utility.Config import Config, makeConfig
        config = makeConfig( "Configuration", "global configuration parameters.\nthis is a catch all section." )
        config.addOption('SCRIPTS_PATH','Ganga/scripts',"""the search path to scripts directory.
When running a script from the system shell (e.g. ganga script) this path is used to search for script""")
        
        config.addOption('LOAD_PATH', '', "the search path for the load() function")
        config.addOption('RUNTIME_PATH','',"""path to runtime plugin packages where custom handlers may be added.
Normally you should not worry about it.
If an element of the path is just a name (like in the example below)
then the plugins will be loaded using current python path. This means that
some packages such as GangaTest may be taken from the release area.""",
                         examples="""RUNTIME_PATH = GangaGUI
RUNTIME_PATH = /my/SpecialExtensions:GangaTest """)

        config.addOption('TextShell','IPython',""" The type of the interactive shell: IPython (cooler) or Console (limited)""")
        config.addOption('StartupGPI','','block of GPI commands executed at startup')
        config.addOption('ReleaseNotes',False,'Flag to print out the relevent subsection of release notes for each experiment at start up')
        config.addOption('gangadir',Ganga.Utility.Config.expandvars(None,'~/gangadir'),'Location of local job repositories and workspaces. Default is ~/gangadir but in somecases (such as LSF CNAF) this needs to be modified to point to the shared file system directory.',filter=Ganga.Utility.Config.expandvars)
        config.addOption('repositorytype','LocalXML','Type of the repository.',examples='LocalXML')
        config.addOption('workspacetype','LocalFilesystem','Type of workspace. Workspace is a place where input and output sandbox of jobs are stored. Currently the only supported type is LocalFilesystem.')

        config.addOption('user','','User name. The same person may have different roles (user names) and still use the same gangadir. Unless explicitly set this option defaults to the real user name.')
        config.addOption('resubmitOnlyFailedSubjobs', True , 'If TRUE (default), calling job.resubmit() will only resubmit FAILED subjobs. Note that the auto_resubmit mechanism will only ever resubmit FAILED subjobs.')
        config.addOption('deleteUnusedShareDir', 'ask' , 'If set to ask (default) the user is presented with a prompt asking whether Shared directories not associated with a persisted Ganga object should be deleted upon Ganga exit. If set to never, shared directories will not be deleted upon exit, even if they are not associated with a persisted Ganga object. If set to always, then shared directories will always be deleted if not associated with a persisted Ganga object.')

        # detect default user (equal to unix user name)
        import getpass
        try:
           config.options['user'].default_value = getpass.getuser()
        except Exception,x:
           raise Ganga.Utility.Config.ConfigError('Cannot get default user name'+str(x))
        
        gpiconfig = Ganga.Utility.Config.makeConfig('GPI_Semantics','Customization of GPI behaviour. These options may affect the semantics of the Ganga GPI interface (what may result in a different behaviour of scripts and commands).')

        gpiconfig.addOption('job_submit_keep_going', False, 'Keep on submitting as many subjobs as possible. Option to j.submit(), see Job class for details')
        gpiconfig.addOption('job_submit_keep_on_fail', False,'Do not revert job to new status even if submission failed. Option to j.submit(), see Job class for details')

        ipconfig = Ganga.Utility.Config.makeConfig('TextShell_IPython','''IPython shell configuration
See IPython manual for more details:
http://ipython.scipy.org/doc/manual''')
        try:
           from IPython import __version__ as ipver
        except ImportError:
           ipver="0.6.13"
        if ipver == "0.6.13": #in older ipython version the option is -noautocall (this is the version shipped with Ganga in 06/2009)
           noautocall = "'-noautocall'"
        else:
           noautocall = "'-autocall','0'"

        ipconfig.addOption('args',"['-colors','LightBG', %s]"%noautocall,'FIXME') 

        # import configuration from spyware
        import spyware

        import Ganga.Utility.ColourText
        
        disply_config = makeConfig('Display', """control the content and appearence of printing ganga objects: attributes,colours,etc.
If ANSI text colours are enabled, then individual colours may be specified like this:
 fg.xxx - Foreground: %s
 bg.xxx - Background: %s
 fx.xxx - Effects: %s
        """ % (Ganga.Utility.ColourText.Foreground.__doc__, Ganga.Utility.ColourText.Background.__doc__,Ganga.Utility.ColourText.Effects.__doc__ ))
        
        #[Shell] section
        shellconfig = makeConfig( "Shell", "configuration parameters for internal Shell utility." )
        shellconfig.addOption('IgnoredVars',['_','SHVL','PWD'],'list of env variables not inherited in Shell environment')

        #[Output] section
        outputconfig = makeConfig( "Output", "configuration section for postprocessing the output" )

        outputconfig.addOption('OutputSandboxFile', {'fileExtensions':[], 'backendPostprocess':{'LSF':'WNclient', 'Localhost':'WN'}},'fileExtensions:list of output files that will be written to mass storage after job is completed, backendPostprocess:defines where postprocessing should be done (WN/client) on different backends')

        outputconfig.addOption('MassStorageFile', {'fileExtensions':['*.dummy'], 'backendPostprocess':{'LSF':'WN', 'LCG':'client', 'CREAM':'client', 'Localhost':'WN'}},'fileExtensions:list of output files that will be written to mass storage after job is completed, backendPostprocess:defines where postprocessing should be done (WN/client) on different backends')

        outputconfig.addOption('LCGStorageElementFile',{'fileExtensions':['*.root'], 'backendPostprocess':{'LSF':'client', 'LCG':'WN', 'CREAM':'WN', 'Localhost':'WN'}, 'uploadOptions':{'LFC_HOST':'lfc-dteam.cern.ch', 
'dest_SRM':'srm-public.cern.ch'}},'fileExtensions:list of output files that will be written to LCG SE, backendPostprocess:defines where postprocessing should be done (WN/client) on different backends, uploadOptions:config values needed for the actual LCG upload')

        #[MassStorageOutput] section
        outputconfig = makeConfig( "MassStorageOutput", "configuration section for storing of the output to a mass storage" )
        outputconfig.addOption('mkdir_cmd', 'nsmkdir', 'Command used to create a directory in the mass storage location')
        outputconfig.addOption('cp_cmd', 'rfcp', 'Command used to copy out data to the mass storage location')
        outputconfig.addOption('ls_cmd', 'nsls', 'Command used to list files in the mass storage location')
        try:
            outputconfig.addOption('path', os.path.join(os.environ['CASTOR_HOME'], 'ganga'), 'path to the mass storage location where the files will be stored')
        except: 
            from Ganga.Utility.Config import getConfig
            user = getConfig('Configuration')['user']   
            massStoragePath = "/castor/cern.ch/user/%s/%s/ganga" % (user[0], user)      
            outputconfig.addOption('path', massStoragePath, 'path to the mass storage location where the files will be stored(if you set the env variable CASTOR_HOME to your home directory in castor, you can configure the path to be $CASTOR_HOME/ganga)')
                
        #[LCGStorageElementOutput] section
        """
        outputconfig = makeConfig( "LCGStorageElementOutput", "configuration section for storing of the output to LCG storage element" )
        outputconfig.addOption('LFC_HOST', 'lfc-dteam.cern.ch', 'LFC host for Logical File Name association with the uploaded output file')
        outputconfig.addOption('dest_SRM', 'srm-public.cern.ch', 'SRM where the output file should be uploaded')
        """

        # all relative names in the path are resolved wrt the _gangaPythonPath
        # the list order is reversed so that A:B maintains the typical path precedence: A overrides B
        # because the user config file is put at the end it always may override everything else
        config_files = Ganga.Utility.Config.expandConfigPath(self.options.config_path,_gangaPythonPath)
        config_files.reverse()
        config_files.append(self.options.config_file)

        # read-in config files

        #FIXME: need to construct a proper dictionary - cannot use the ConfigPackage directly
        system_vars = {}
        for opt in syscfg:
           system_vars[opt]=syscfg[opt]

        Ganga.Utility.Config.configure(config_files,system_vars)

        # set the system variables to the [System] module
        #syscfg.setDefaultOptions(system_vars,reset=1)

        # activate the logging subsystem
        Ganga.Utility.logging.bootstrap() # user defined log level takes effect NOW

        if not self.options.monitoring:
            self.options.cmdline_options.append('[PollThread]autostart=False')
        self.logger = Ganga.Utility.logging.getLogger(modulename=True)
        self.logger.debug('default user name is %s',config['user'])
        self.logger.debug('user specified cmdline_options: %s',str(self.options.cmdline_options))
        
        # override the config options from the command line arguments
        # the format is [section]option=value OR option=value
        # in the second case last specified section from previous options is used

        set_cmdline_config_options()

        self.new_user_wizard()

        if self.options.GUI:
           ## FIXME: CONFIG CHECK
           ## ??? config['RUNTIME_PATH'] = ''
           config.setSessionValue('TextShell','GUI')
           config.setSessionValue('RUNTIME_PATH','GangaGUI')
           
        if self.options.TEST:
           ## FIXME: CONFIG CHECK           
           ## ?? config['RUNTIME_PATH'] = ''
           config.setSessionValue('RUNTIME_PATH','GangaTest')

    # initialize environment: find all user-defined runtime modules and set their environments
    # if option rexec=1 then initEnvironment restarts the current ganga process (needed for LD_LIBRARY_PATH on linux)
    # set rexec=0 if you prepare your environment outside of Ganga and you do not want to rexec process
    def initEnvironment(self):

        from Ganga.Core.InternalServices import ShutdownManager
        ShutdownManager.install()
       
        import os,os.path
        import Ganga.Utility.Config
        from Ganga.Utility.Runtime import RuntimePackage, allRuntimes
        from Ganga.Core import GangaException
        
        try:
           # load Ganga system plugins...
           import plugins
        except Exception,x:
           self.logger.critical('Ganga system plugins could not be loaded due to the following reason: %s',str(x))
           self.logger.exception(x)
           raise GangaException(x) 

        # initialize runtime packages, they are registered in allRuntimes dictionary automatically
        try:
            import Ganga.Utility.files
            config = Ganga.Utility.Config.getConfig('Configuration')
            
            #runtime warnings issued by the interpreter may be suppresed
            #config['IgnoreRuntimeWarnings'] = False
            config.addOption('IgnoreRuntimeWarnings', False, "runtime warnings issued by the interpreter may be suppresed")
            if config['IgnoreRuntimeWarnings']:
               import warnings
               warnings.filterwarnings(action="ignore", category=RuntimeWarning)

            def transform(x):
               return os.path.normpath(Ganga.Utility.files.expandfilename(x))
            
            paths = map(transform,filter(lambda x:x, config['RUNTIME_PATH'].split(':')))

            for path in paths:
                r = RuntimePackage(path)
        except KeyError:
            pass

        # initialize the environment only if the current ganga process has not been rexeced
        if not os.environ.has_key('GANGA_INTERNAL_PROCREEXEC') and not os.environ.has_key('GANGA_NEVER_REEXEC'):
            self.logger.debug('initializing runtime environment')
            # update environment of the current process
            for r in allRuntimes.values():
                try:
                    _env = r.getEnvironment()
                    if _env:
                       os.environ.update(_env)
                except Exception,x:
                   Ganga.Utility.logging.log_user_exception()
                   self.logger.error("can't get environment for %s, possible problem with the return value of getEvironment()",r.name,)
                   raise


            # in some cases the reexecution of the process is needed for LD_LIBRARY_PATH to take effect
            # re-exec the process if it is allowed in the options
            if self.options.rexec:
                self.logger.debug('re-executing the process for LD_LIBRARY_PATH changes to take effect')
                os.environ['GANGA_INTERNAL_PROCREEXEC'] = '1'
                prog = os.path.normpath(sys.argv[0])
                os.execv(prog,sys.argv)

        else:
            self.logger.debug('skipped the environment initialization -- the processed has been re-execed and setup was done already')

        #bugfix 40110
        if os.environ.has_key('GANGA_INTERNAL_PROCREEXEC'):
           del os.environ['GANGA_INTERNAL_PROCREEXEC']
        
    # bootstrap all system and user-defined runtime modules
    def bootstrap(self):
        import Ganga.Utility.Config
        config = Ganga.Utility.Config.getConfig('Configuration')

        from Ganga.Core import GangaException
        from Ganga.Utility.Runtime import allRuntimes
        import Ganga.Utility.logging

        # load user-defined plugins...
        for r in allRuntimes.values():
            try:
               r.loadPlugins()
            except Exception,x:
                Ganga.Utility.logging.log_user_exception()
                self.logger.error("problems with loading plugins for %s -- ignored",r.name,)

        from GPIexport import exportToGPI
        
        from Ganga.Utility.Plugin import allPlugins

        # make all plugins visible in GPI
        for k in allPlugins.allCategories():
            for n in allPlugins.allClasses(k):
               cls = allPlugins.find(k,n)
               if not cls._declared_property('hidden'):
                  exportToGPI(n,cls._proxyClass,'Classes')

        # set the default value for the plugins

        default_plugins_cfg = Ganga.Utility.Config.makeConfig('Plugins','''General control of plugin mechanism.
Set the default plugin in a given category.
For example:
default_applications = DaVinci
default_backends = LCG
''')

        for opt in default_plugins_cfg:
           try:
              category,tag = opt.split('_')
           except ValueError:
              self.logger.warning("do not understand option %s in [Plugins]",opt)
           else:
              if tag == 'default':
                 try:
                    allPlugins.setDefault(category,default_plugins_cfg[opt])
                 except Ganga.Utility.Plugin.PluginManagerError,x:
                    self.logger.warning('cannot set the default plugin "%s": %s',opt,x)
              else:
                 self.logger.warning("do not understand option %s in [Plugins]",opt) 

        # set alias for default Batch plugin (it will not appear in the configuration)

        batch_default_name = Ganga.Utility.Config.getConfig('Configuration').getEffectiveOption('Batch')
        try:
           batch_default = allPlugins.find('backends',batch_default_name)
        except Exception,x:
           raise Ganga.Utility.Config.ConfigError('Check configuration. Unable to set default Batch backend alias (%s)'%str(x))
        else:
           allPlugins.add(batch_default,'backends','Batch')
           exportToGPI('Batch',batch_default._proxyClass,'Classes')
        
        from Ganga.GPIDev.Base import ProtectedAttributeError, ReadOnlyObjectError, GangaAttributeError
        from Ganga.GPIDev.Lib.Job.Job import JobError

        exportToGPI('GangaAttributeError',GangaAttributeError,'Exceptions')
        exportToGPI('ProtectedAttributeError',ProtectedAttributeError,'Exceptions')
        exportToGPI('ReadOnlyObjectError',ReadOnlyObjectError,'Exceptions')
        exportToGPI('JobError',JobError,'Exceptions')

        # initialize external monitoring services subsystem
        import Ganga.GPIDev.MonitoringServices

        def license():
           'Print the full license (GPL)'
           print file(os.path.join(_gangaPythonPath,'..','LICENSE_GPL')).read()
           
        exportToGPI('license',license,'Functions')
        # bootstrap credentials

        from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
        from Ganga.GPIDev.Credentials import getCredential 
        
        # only the available credentials are exported
        
        # At this point we expect to have the GridProxy already created 
        # by one of the Grid plugins (LCG/NG/etc) so we search for it in creds cache
        credential = getCredential(name = 'GridProxy', create = False)
        if credential:
            exportToGPI('gridProxy',GPIProxyObjectFactory(credential),'Objects','Grid proxy management object.')
            
        credential = getCredential('AfsToken')
        if credential:
            exportToGPI('afsToken',GPIProxyObjectFactory(credential),'Objects','AFS token management object.')
        
        # add built-in functions

        from Ganga.GPIDev.Persistency import export, load
        exportToGPI('load',load,'Functions')
        exportToGPI('export',export,'Functions')
        
        def typename(obj):
            'Return a name of Ganga object as a string, example: typename(j.application) -> "DaVinci"'
            return obj._impl._name

        def categoryname(obj):
            'Return a category of Ganga object as a string, example: categoryname(j.application) -> "applications"'
            return obj._impl._category

        def plugins(category=None):
           """List loaded plugins.

           If no argument is given return a dictionary of all loaded plugins.
           Keys are category name. Values are lists of plugin names in each
           category.

           If a category is specified (for example 'splitters') return a list
           of all plugin names in this category.
           """
           from Ganga.Utility.Plugin import allPlugins
           if category:
              return allPlugins.allClasses(category).keys()
           else:
              d = {}
              for c in allPlugins.allCategories():
                 d[c] = allPlugins.allCategories()[c].keys()
              return d

        ### FIXME: DEPRECATED
        def list_plugins(category):
            'List all plugins in a given category, OBSOLETE: use plugins(category)'
            self.logger.warning('This function is deprecated, use plugins("%s") instead',category)
            from Ganga.Utility.Plugin import allPlugins
            return allPlugins.allClasses(category).keys()
        
        def applications():
            'return a list of all available applications, OBSOLETE: use plugins("applications")'
            return list_plugins('applications')

        def backends():
            'return a list of all available backends, OBSOLETE: use plugins("backends")'
            return list_plugins('backends')

        exportToGPI('applications',applications,'Functions')
        exportToGPI('backends',backends,'Functions')
        exportToGPI('list_plugins',list_plugins,'Functions')
        ### FIXME: END DEPRECATED
         
        exportToGPI('typename',typename,'Functions')
        exportToGPI('categoryname',categoryname,'Functions')
        exportToGPI('plugins',plugins,'Functions')


        def force_job_completed(j):
           "obsoleted, use j.force_status('completed') instead"
           raise GangaException("obsoleted, use j.force_status('completed') instead")

        def force_job_failed(j):
           "obsoleted, use j.force_status('failed') instead"
           raise GangaException("obsoleted, use j.force_status('failed') instead")

        exportToGPI('force_job_completed',force_job_completed,'Functions')
        exportToGPI('force_job_failed',force_job_failed,'Functions')
        
        # import default runtime modules
        import Repository_runtime
        import Ganga.Core
        import associations

        # bootstrap user-defined runtime modules

        for n,r in zip(allRuntimes.keys(),allRuntimes.values()):
            try:
                r.bootstrap(Ganga.GPI.__dict__)
            except Exception,x:
                Ganga.Utility.logging.log_user_exception()
                self.logger.error('problems with bootstrapping %s -- ignored',n)
        
        # bootstrap runtime modules
        import Ganga.GPIDev.Lib.Registry
        from Ganga.GPIDev.Lib.JobTree import JobTree,TreeError
        import Ganga.GPIDev.Lib.Tasks 

        # boostrap the repositories and connect to them
        for n,k,d in Repository_runtime.bootstrap():
            # make all repository proxies visible in GPI
            exportToGPI(n,k,'Objects',d)
       
        # JobTree 
        from Ganga.Core.GangaRepository import getRegistry
        jobtree = GPIProxyObjectFactory(getRegistry("jobs").getJobTree())
        exportToGPI('jobtree',jobtree,'Objects','Logical tree view of the jobs')
        exportToGPI('TreeError',TreeError,'Exceptions')

        # ShareRef
        shareref = GPIProxyObjectFactory(getRegistry("prep").getShareRef())
        exportToGPI('shareref',shareref,'Objects','Mechanism for tracking use of shared directory resources')

        # bootstrap the workspace
        import Workspace_runtime
        Workspace_runtime.bootstrap()

        # migration repository
        #from Ganga.Utility.migrate41to42 import JobCheckForV41, JobConvertToV42
        #JobCheckForV41()
        #exportToGPI('JobConvertToV42',JobConvertToV42,'Functions')

        #export full_print
        from Ganga.GPIDev.Base.VPrinter import full_print
        exportToGPI('full_print',full_print,'Functions')
        
        # bootstrap core modules
        Ganga.Core.bootstrap(Ganga.GPI.jobs._impl,self.interactive)

        import Ganga.GPIDev.Lib.Config
        exportToGPI('config',Ganga.GPIDev.Lib.Config.config,'Objects','access to Ganga configuration')
        exportToGPI('ConfigError',Ganga.GPIDev.Lib.Config.ConfigError,'Exceptions')

        from Ganga.Utility.feedback_report import report

        exportToGPI('report',report,'Functions')

        # export all configuration items, new options should not be added after this point
        Ganga.GPIDev.Lib.Config.bootstrap()


        ###########
        # run post bootstrap hooks
        for r in allRuntimes.values():
            try:
               r.postBootstrapHook()
            except Exception,x:
                Ganga.Utility.logging.log_user_exception()
                self.logger.error("problems with post bootstrap hook for %s",r.name,)
        
    def startTestRunner(self):
       """
       run the testing framework
       """
       
       try:
          from GangaTest.Framework import runner
          from GangaTest.Framework import htmlizer
          from GangaTest.Framework import xmldifferencer
          
          tfconfig = Ganga.Utility.Config.getConfig('TestingFramework')                    
          rc = 1    
          if tfconfig['EnableTestRunner']:          
            self.logger.info("Starting Ganga Test Runner")
          
            if not self.args:
                self.logger.warning("Please specify the tests to run ( i.e. ganga --test Ganga/test )")
                return -1
          
            rc = runner.start(test_selection=" ".join(self.args))
          else:
            self.logger.info("Test Runner is disabled (set EnableTestRunner=True to enable it)")    
          
          if rc > 0 and tfconfig['EnableHTMLReporter']:                          
             self.logger.info("Generating tests HTML reports")
             rc = htmlizer.main(tfconfig)
          elif rc > 0 and tfconfig['EnableXMLDifferencer']:
             self.logger.info("Generating difference HTML reports")
             rc = xmldifferencer.main(self.args)
          return rc
       except ImportError,e:
          self.logger.error("You need GangaTest external package in order to invoke Ganga test-runner.")
          print e
          return -1

    # run Ganga in the specified namespace, in principle the namespace should import all names from Ganga.GPI
    # if namespace is not specified then run in __main__
    def run(self,local_ns=None):

        if self.options.webgui == True:
            from Ganga.Runtime.http_server import start_server
            start_server()

        def override_credits():
           credits._Printer__data += '\n\nGanga: The Ganga Developers (http://cern.ch/ganga)\n'
           copyright._Printer__data += '\n\nCopyright (c) 2000-2008 The Ganga Developers (http://cern.ch/ganga)\n'

        if local_ns is None:
            import __main__
            local_ns = __main__.__dict__
        #save a reference to the Ganga namespace as an instance attribute
        self.local_ns = local_ns
            
        # load templates for user-defined runtime modules
        from Ganga.Utility.Runtime import allRuntimes
        for r in allRuntimes.values():
            r.loadTemplates( local_ns )
        
        # exec ~/.ganga.py file
        fileName = fullpath('~/.ganga.py')
        if os.path.exists(fileName):
            try:
                execfile( fileName, local_ns )
            except Exception, x:
                self.logger.error('Failed to source %s (Error was "%s"). Check your file for syntax errors.', fileName, str(x))
        # exec StartupGPI code          
        from Ganga.Utility.Config import getConfig      
        config=getConfig('Configuration')       
        # exec StartupGPI code          
        from Ganga.Utility.Config import getConfig      
        config=getConfig('Configuration')
        if config['StartupGPI']:
           #ConfigParser trims the lines and escape the space chars
           #so we have only one possibility to insert python code : 
           # using explicitly '\n' and '\t' chars
           code = config['StartupGPI'].replace('\\t','\t').replace('\\n','\n')
           exec code in local_ns
          

        #Go through file line by line, use the 'name' to find where the experiment specific notes are.
        def printreleasenotes(filelist, name, version):
            startprinting = 100000
            stopprinting = 100000
            for linenumber, line in enumerate(filelist):  
                if line.find(name) > 0:
                    print name.lstrip('python/').rstrip('\n')+' release notes for version '+version+':\n'
                    startprinting = linenumber+2
                if startprinting < linenumber and stopprinting > linenumber and line[1:5] == '----': 
                    stopprinting = linenumber-2
            for line in filelist[startprinting:stopprinting]:
                print line,
           #If there is no release notes:
            if stopprinting-startprinting < 1:
                print 'No release notes for '+name.lstrip('python/').rstrip('\n')+' in version '+version+'\n'
            else: 
                print ' '

        #Find out if ganga version has been used before by writing to a hidden file in the gangadir
        def new_version(version):
            _new_version = True
            versionfile_path = config['gangadir']+'/.used_versions'
            if os.path.isfile(versionfile_path):
                f_version = open(versionfile_path,'r+')
                for line in f_version:
                    if version == line:
                        _new_version = False
                if _new_version == True:
                    f_version.write(version)
                f_version.close()
            else:
                f_version = open(versionfile_path,'w')
                f_version.write(version)
                f_version.close()
            return _new_version

        #print release notes
        if config['ReleaseNotes']==True:
            runtime = config['RUNTIME_PATH']
            #name = runtime[runtime.find(':')+1:len(runtime)-1]
            name = [n for n in runtime.strip().split(':') if n is not '']
            import commands
            version = commands.getoutput('ganga --version').lstrip("Ganga-")
            version = version.replace('-','.')
            installdir = commands.getoutput('which ganga').rstrip("/InstallArea/scripts/ganga")
            relnotespath =installdir+'/install/ganga/release/ReleaseNotes-'+version
            if new_version(version+'\n') == True and os.path.isfile(relnotespath):
                f_releasenotes = open(installdir+'/install/ganga/release/ReleaseNotes-'+version)
                filelist = f_releasenotes.readlines()
                print '\n*****************************************************************************'
                printreleasenotes(filelist,'python/Ganga'+'\n',version)
                for n in name: printreleasenotes(filelist,'python/'+n+'\n',version)
                print '*****************************************************************************'
                f_releasenotes.close()
 
        # monitor the  ganga usage
        import spyware

        # this logic is a bit convoluted
        runs_script = len(self.args)>0
        session_type = config['TextShell']
        if runs_script:
           if not self.interactive:
              session_type = 'batch'
           else:
              session_type += 'startup_script'
              
        spyware.ganga_started(session_type,interactive=self.interactive,GUI=self.options.GUI,webgui=self.options.webgui,script_file=runs_script, text_shell=config['TextShell'],test_framework=self.options.TEST)
        
        if self.options.TEST:
            sys.argv = self.args
            try:
               rc = self.startTestRunner()             
            except (KeyboardInterrupt, SystemExit):
               self.logger.warning('Test Runner interrupted!')
               sys.exit(1)
            sys.exit(rc)

        if len(self.args) > 0:
            # run the script and make it believe it that it is running directly as an executable (sys.argv)
            saved_argv = sys.argv
            sys.argv =  self.args

            import Ganga.Utility.Runtime
            path = Ganga.Utility.Runtime.getSearchPath()
            script = Ganga.Utility.Runtime.getScriptPath( self.args[ 0 ], path )
                  
            if script:         
               execfile( script, local_ns )
            else:
               self.logger.error( "'%s' not found" % self.args[ 0 ] )
               self.logger.info( "Searched in path %s" % path )
               sys.exit(1)
            sys.argv = saved_argv

        # and exit unless -i was specified
        if not self.interactive:
            sys.exit(0)

        # interactive python shell

        # customized display hook -- take advantage of coloured text etc. if possible.
        def _display(obj):
           if isinstance(obj,type):
              print
              print obj
              return
           if hasattr(obj,'_display'):
              print
              print obj._display(1)
              return
           if hasattr(obj,'_impl') and hasattr(obj._impl,'_display'):
              print
              print obj._display(1)
              return
           print obj

        shell = config['TextShell']

        import Ganga.Utility.Config.Config
        #Ganga.Utility.Config.Config.sanityCheck()
        
        if shell == 'IPython':
            ipconfig = Ganga.Utility.Config.getConfig('TextShell_IPython')
#            ipconfig = Ganga.Utility.Config.makeConfig('TextShell_IPython','IPython shell configuration')
#            ipconfig.addOption('args',"['-colors','LightBG', '-noautocall']",'FIXME')
            args = eval(ipconfig['args'])

            try:
               self.logger.warning('Environment variable IPYTHONDIR=%s exists and overrides the default history file for Ganga IPython commands',os.environ['IPYTHONDIR'])
            except KeyError:
               newpath = os.path.expanduser('~/.ipython-ganga')
               oldpath = os.path.expanduser('~/.ipython')
               os.environ['IPYTHONDIR'] = newpath
               if not os.path.exists(newpath) and os.path.exists(oldpath):
                  self.logger.warning('Default location of IPython history files has changed.')
                  self.logger.warning('Ganga will now try to copy your old settings from %s to the new path %s. If you do not want that, quit Ganga and wipe off the content of new path: rm -rf %s/*',oldpath,newpath,newpath)
                  import shutil
                  shutil.copytree(oldpath,newpath)
               

            # buffering of log messages from all threads called "GANGA_Update_Thread"
            # the logs are displayed at the next IPython prompt
            
            from Ganga.Utility.logging import enableCaching

            import Ganga.Utility.logging
            Ganga.Utility.logging.enableCaching()

            def ganga_prompt():
               if Ganga.Utility.logging.cached_screen_handler:
                  Ganga.Utility.logging.cached_screen_handler.flush()

               credentialsWarningPrompt = ''
               #alter the prompt only when the internal services are disabled
               from Ganga.Core.InternalServices import Coordinator
               if not Coordinator.servicesEnabled:
                  invalidCreds = Coordinator.getMissingCredentials()
                  if invalidCreds:
                     credentialsWarningPrompt = '[%s required]' % ','.join(invalidCreds)
                  if credentialsWarningPrompt: # append newline
                     credentialsWarningPrompt+='\n'
               
               return credentialsWarningPrompt
            
            from IPython.Shell import IPShellEmbed          
            #override ipothonrc configuration  
            ipopts = {'prompt_in1':'${ganga_prompt()}In [\#]:',
                      'readline_omit__names':2 # disable automatic tab completion for attributes starting with _ or __
                     }
            ipshell = IPShellEmbed(argv=args,rc_override=ipopts)
            # setting displayhook like this is definitely undocumented sort of a hack
            ipshell.IP.outputcache.display = _display
            ipshell.IP.user_ns['ganga_prompt'] = ganga_prompt

            # attach magic functions
            import IPythonMagic

            #set a custom exception handler wich disables printing of errors' traceback for 
            #all exceptions inheriting from GangaException
            def ganga_exc_handler(self,etype,value,tb):
                #print str(etype).split('.')[-1],':', # FIXME: sys.stderr ?
                print '\n',value, # FIXME: sys.stderr ?
            from Ganga.Core import GangaException
            ipshell.IP.set_custom_exc((GangaException,),ganga_exc_handler)
            override_credits()
            ret = ipshell(local_ns=local_ns,global_ns=local_ns) #global_ns: FIX required by ipython 0.8.4+
        elif shell == 'GUI':
           override_credits()
           import GangaGUI.Ganga_GUI
           GangaGUI.Ganga_GUI.main()
        else:
            override_credits()
            import code
            sys.displayhook = _display
            c = code.InteractiveConsole(locals=local_ns)
            c.interact()

    def log(self,x):

       import sys
       # FIXME: for some reason self.logger.critical does not print any messages here
       if self.options.force_loglevel == 'DEBUG':
          import traceback
          traceback.print_exc(file=sys.stderr)
       else:
          print >>sys.stderr, x
          print >>sys.stderr, '(consider --debug option for more information)'

#
#
# $Log: not supported by cvs2svn $
# Revision 1.11.4.1  2009/07/08 11:18:21  ebke
# Initial commit of all - mostly small - modifications due to the new GangaRepository.
# No interface visible to the user is changed
#
# Revision 1.11  2009/04/28 13:37:12  kubam
# simplified handling of logging filters
#
# Revision 1.15  2009/07/20 14:13:44  moscicki
# workaround for wierd OSX execv behaviour (from Ole Weidner)
#
# Revision 1.14  2009/06/10 14:53:05  moscicki
# fixed bug #51592: Add self to logger
#
# Revision 1.13  2009/06/09 10:44:55  moscicki
# removed obsolete variable
#
# Revision 1.12  2009/06/08 15:48:17  moscicki
# fix Ganga to work with newer versions of ipython (-noautocall option was removed in newer ipython versions)
#
# Revision 1.11  2009/04/28 13:37:12  kubam
# simplified handling of logging filters
#
# Revision 1.10  2009/02/02 13:43:26  moscicki
# fixed: bug #44934: Didn't create .gangarc on first usage
#
# Revision 1.9  2008/11/27 15:49:03  moscicki
# extra exception output if cannot load the plugins...
#
# Revision 1.8  2008/11/21 16:34:22  moscicki
# bug #43917: Implement Batch backend as alias to default backend at a given site
#
# Revision 1.7  2008/10/23 15:24:04  moscicki
# install the shutdown manager for atexit handlers before loading system plugins (e.g. LCG download thread registers the atexit handler using a tuple (priority,handler))
#
# Revision 1.6  2008/09/05 15:55:51  moscicki
# XML differenciater added (from Ulrik)
#
# Revision 1.5  2008/08/18 13:18:59  moscicki
# added force_status() method to replace job.fail(), force_job_failed() and
# force_job_completed()
#
# Revision 1.4  2008/08/18 10:02:15  moscicki
#
# bugfix 40110
#
# Revision 1.3  2008/08/01 15:25:30  moscicki
# typo fix
#
# Revision 1.2  2008/07/31 17:25:02  moscicki
# config templates are now in a separate directory at top level ("templates")
#
# *converted all tabs to spaces*
#
# Revision 1.1  2008/07/17 16:41:00  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.71.4.23  2008/07/03 16:11:31  moscicki
# bug #38000: Add check for old .gangarc file
#
# Revision 1.71.4.22  2008/04/03 12:55:45  kuba
# importing core plugins before initEnvironment(), this fixes
# bug #35146: GangaAtlas is not starting due to gridshell call in __init__.py
#
# Revision 1.71.4.21  2008/04/01 14:08:03  roma
# automatic config file template generation (Vladimir)
#
# Revision 1.71.4.20  2008/03/31 15:32:46  kubam
# use more flexible logic for hidden classes
#
# Revision 1.71.4.19  2008/03/12 17:33:32  moscicki
# workaround for broken logging system: GangaProgram.log writes directly to stderr
#
# Revision 1.71.4.18  2008/03/11 15:24:51  moscicki
# merge from Ganga-5-0-restructure-config-branch
#
# Revision 1.71.4.17.2.1  2008/03/07 13:34:38  moscicki
# workspace component
#
# Revision 1.71.4.17  2008/03/06 14:14:55  moscicki
# streamlined session options logic
# moved sanityCheck to config.bootstrap()
#
# Revision 1.71.4.16  2008/03/06 11:26:52  amuraru
# fixed .ganga.py sourcing file
#
# Revision 1.71.4.15  2008/03/05 14:53:33  amuraru
# execute ~/.ganga.py file before executing StartupGPI code
#
# Revision 1.71.4.14  2008/02/28 10:08:32  amuraru
# *** empty log message ***
#
# Revision 1.71.4.13  2008/02/21 12:11:02  amuraru
# added [Shell] section to configure internal Shell utility
#
# Revision 1.71.4.12  2008/02/06 17:04:11  moscicki
# initialize external monitoring services subsystem
#
# Revision 1.71.4.11  2007/12/18 16:51:28  moscicki
# merged from XML repository branch
#
# Revision 1.71.4.10  2007/12/18 13:05:19  amuraru
# removed coverage code from boostrap (moved in GangaTest/Framework/driver.py)
#
# Revision 1.71.4.9  2007/12/13 16:33:02  moscicki
# export more GPI exceptions
#
# Revision 1.71.4.8  2007/12/10 18:55:55  amuraru
# merged changes from Ganga 4.4.4
#
# Revision 1.71.4.7  2007/11/14 11:41:46  amuraru
# 5.0 configuration updated
#
# Revision 1.71.4.6.2.1  2007/11/13 16:26:05  moscicki
# removed obsolete migration GPI commands
#
# Revision 1.71.4.6  2007/11/08 13:21:00  amuraru
# moved testconfig option defintion to GangaTest
#
# Revision 1.71.4.5  2007/11/07 15:10:04  moscicki
# merged in pretty print and GangaList support from ganga-5-dev-branch-4-4-1-will-print branch
#
#
# Revision 1.71.4.4  2007/11/02 15:20:32  moscicki
# moved addOption() before config bootstrap
#
# Revision 1.71.4.3  2007/10/31 13:39:45  amuraru
# update to the new config system
#
# Revision 1.71.4.2  2007/10/25 11:43:11  roma
# Config update
#
# Revision 1.71.4.1  2007/10/12 13:56:26  moscicki
# merged with the new configuration subsystem
#
# Revision 1.71.6.2  2007/10/09 07:31:56  roma
# Migration to new Config
#
# Revision 1.71.6.1  2007/09/25 09:45:12  moscicki
# merged from old config branch
#
# Revision 1.71.8.1  2007/10/30 12:12:08  wreece
# First version of the new print_summary functionality. Lots of changes, but some known limitations. Will address in next version.
#
# Revision 1.76  2007/11/26 12:13:27  amuraru
# decode tab and newline characters in StartupGPI option
#
# Revision 1.75  2007/11/05 12:33:54  amuraru
# fix bug #30891
#
# Revision 1.74  2007/10/29 14:04:08  amuraru
#  - added free disk space checking in [PollThread] configuration template
#  - added an extra check not to attempt the shutdown of the repository if this has already been stopped
#  - save the Ganga namespace as an attribute in Ganga.Runtime._prog
#
# Revision 1.73  2007/10/10 14:47:46  moscicki
# updated doc-strings
#
# Revision 1.72  2007/09/25 15:12:04  amuraru
#
# usa GANGA_CONFIG_FILE  environment variable to set the user config file
#
# Revision 1.71  2007/09/11 16:54:52  amuraru
# catch the TestRunner KeyboardInterrupt
#
# Revision 1.70  2007/09/11 14:28:29  amuraru
# implemented FR #28406 to allow definition of GPI statements to be executed at
# startup
#
# Revision 1.69  2007/08/27 10:47:30  moscicki
# overriden credits() and copyright() (request #21906)
#
# Revision 1.68  2007/08/22 15:58:55  amuraru
# Runtime/bootstrap.py
#
# Revision 1.67  2007/08/14 14:47:01  amuraru
# automatically add GangaTest RT package when --test is used
#
# Revision 1.66  2007/08/13 17:22:27  amuraru
# - testing framework small fix
#
# Revision 1.65  2007/08/13 13:19:48  amuraru
# -added EnableTestRunner and EnableHTMLReported to control the testing framework in a more flexible way
# -added GANGA_CONFIG_FILE in [System] config
#
# Revision 1.64  2007/08/13 12:50:18  amuraru
# added EnableTestRunner and EnableHTMLReported to control the testing framework in a more flexible way
#
# Revision 1.63  2007/07/30 12:57:51  moscicki
# removing IPython autocall option (obsoletion of jobs[] syntax and putting jobs() as a replacement)
#
# Revision 1.62  2007/07/27 14:31:55  moscicki
# credential and clean shutdown updates from Adrian (from Ganga-4-4-0-dev-branch)
#
# Revision 1.61  2007/07/10 13:08:32  moscicki
# docstring updates (ganga devdays)
#
# Revision 1.60  2007/06/07 10:25:02  amuraru
# bug-fix: guard against environment update for RuntimePackages exposing null environment dictionary
#
# Revision 1.59  2007/06/04 14:31:22  amuraru
# record start-time of ganga session
#
# Revision 1.58  2007/06/01 08:49:08  amuraru
# Disable the autocompletion of private attributes and methods starting with _ or __
#
# Revision 1.57  2007/05/21 16:07:57  amuraru
# integrated TestingFramework into ganga itsefl (ganga --test). [TestingFramework] section in Ganga config is used to control it.
# changed Ganga.Runtime.bootstrap default log level to INFO
#
# Revision 1.56  2007/05/11 13:21:24  moscicki
# temporary functions to help getting jobs out of completing and submitting states
# force_job_completed(j): may be applied to completing jobs
# force_job_failed(j): may be applied to submitting or completing jobs
#
# Revision 1.55  2007/05/08 10:32:42  moscicki
# added short GPL license summary at startup and license() command in GPI for full print
#
# Revision 1.54.6.1  2007/06/18 07:44:57  moscicki
# config prototype
#
# Revision 1.54  2007/02/28 18:24:53  moscicki
# moved GangaException to Ganga.Core
#
# Revision 1.53  2007/02/22 13:43:19  moscicki
# pass interactive flag to Core.bootstrap
#
# Revision 1.52  2007/01/25 15:52:39  moscicki
# mergefrom_Ganga-4-2-2-bugfix-branch_25Jan07 (GangaBase-4-14)
#
# Revision 1.51.2.3  2006/12/15 17:12:37  kuba
# added spyware at startup
#
# Revision 1.51.2.2  2006/11/24 14:52:56  amuraru
# only available credentials (afs/gridproxy) are exported
#
# Revision 1.51.2.1  2006/11/24 14:22:05  amuraru
# added support for peek() function
#
# Revision 1.51  2006/10/23 10:59:41  moscicki
# initialize [Configuration]LOAD_PATH
#
# Revision 1.50  2006/10/16 12:53:13  moscicki
# fix the SCRIPTS_PATH mechanism: the . is always in the path and the session level updates are prepending to the default value... fix for bug #20332 overview: Ganga/scripts not included in SCRIPTS_PATH in Atlas.ini
#
# Revision 1.49  2006/10/04 18:16:48  moscicki
# fixed bug #20333 overview: hostname function of Ganga/Utility/util.py sometimes fails
#
# Revision 1.48  2006/09/27 16:38:31  moscicki
# changed AfsToken -> afsToken, GridProxy -> gridProxy and made them real GPI proxy objects
#
# Revision 1.47  2006/09/15 14:23:31  moscicki
# Greeting message goes to stderr (requested by UNOSAT to use Ganga in CGI scripts).
#
# Revision 1.46  2006/08/29 15:11:10  moscicki
# fixed #18084 Additonal global objects for splitters, mergers etc
#
# Revision 1.45  2006/08/29 12:51:57  moscicki
# exported GridProxy and AfsToken singleton objects to GPI
#
# Revision 1.44  2006/08/11 13:13:06  adim
# Added: GangaException as a markup base class for all exception that need to be printed in a usable way in IPython shell
#
# Revision 1.43  2006/08/09 09:07:34  moscicki
# added magic functions ('ganga')
#
# Revision 1.42  2006/07/31 12:13:43  moscicki
# depend on monitoring thread names "GANGA_Update_Thread" to do message buffering in IPython
#
# Revision 1.41  2006/07/27 20:21:24  moscicki
# - fixed option parsing
# - pretty formatting of known exceptions in IPython (A.Muraru)
#
# Revision 1.40  2006/06/21 11:43:00  moscicki
# minor fix
#
# Revision 1.39  2006/03/14 14:53:14  moscicki
# updated comments
#
# Revision 1.38  2006/03/09 08:41:52  moscicki
# --gui option and GUI integration
#
# Revision 1.37  2006/02/13 15:21:25  moscicki
# support for cached logging messages at interactive prompt (messages from monitoring thread are cached in IPython environment and printed at the next prompt)
#
# Revision 1.36  2006/02/10 14:16:00  moscicki
# fixed bugs:
# #13912        Cannot use tilde to give location of INI file
# #14436 problem with -o option at the command line and setting config default for properties
#
# exported ConfigError to GPI
# docstring updates
#
# Revision 1.35  2005/11/25 09:57:37  moscicki
# exported TreeError exception
#
# Revision 1.34  2005/11/14 14:47:38  moscicki
# jobtree added
#
# Revision 1.33  2005/11/14 10:29:10  moscicki
# support for default plugins
# temporary hack for GUI-specific monitoring
#
# Revision 1.32  2005/11/01 11:21:37  moscicki
# support for export/load (KH)
#
# Revision 1.31  2005/10/14 12:54:38  moscicki
# ignore I/O exceptions while checking for ganga3 config file
#
# Revision 1.30  2005/10/12 13:35:23  moscicki
# renamed _gangadir into gangadir
#
# Revision 1.29  2005/10/07 15:08:45  moscicki
# renamed __Ganga4__ into _gangadirm .ganga4 into .gangarc
# added sanity checks to detect old (Ganga3) config files
# added config-path mechanism
#
# Revision 1.28  2005/10/07 08:27:00  moscicki
# all configuration items have default values
#
# Revision 1.27  2005/09/22 12:48:14  moscicki
# import fix
#
# Revision 1.26  2005/09/21 09:12:50  moscicki
# added interactive displayhooks based on obj._display (if exists)
#
# Revision 1.25  2005/08/26 10:12:06  moscicki
# added [System] section (write-protected) with GANGA_VERSION and GANGA_PYTHONPATH (new)
#
# Revision 1.24  2005/08/24 15:24:11  moscicki
# added docstrings for GPI objects and an interactive ganga help system based on pydoc
#
#
#

