##########################################################################
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
##########################################################################

import os
import os.path
import string
import sys
import time
import re
import atexit

from Ganga import _gangaVersion, _gangaPythonPath
from Ganga.Utility.Config.Config import getConfig
from Ganga.Utility import stacktracer
import Ganga.Runtime

def new_version_format_to_old(version):
    """
    Convert from 'x.y.z'-style format to 'Ganga-x-y-z'

    Example:
        >>> new_version_format_to_old('6.1.11')
        'Ganga-6-1-11'
    """
    return 'Ganga-'+version.replace('.', '-')



from Ganga.Utility.files import fullpath

# This code can help debugging when files aren't closed correctly and
# managing I/O

DEBUGFILES = False
MONITOR_FILES = False

if DEBUGFILES or MONITOR_FILES:
    import __builtin__
    openfiles = {}
    oldfile = __builtin__.file

    class newfile(oldfile):

        def __init__(self, *args):
            self.x = args[0]
            if DEBUGFILES:
                from Ganga.Utility.logging import getLogger
                logger = getLogger(modulename=True)
                logger.debug("init")
                logger.debug("### OPENING %s ###" % self.x)
            oldfile.__init__(self, *args)
            openfiles[self.x] = self

        def close(self):
            if DEBUGFILES:
                from Ganga.Utility.logging import getLogger
                logger = getLogger(modulename=True)
                logger.debug("### CLOSING %s ###" % self.x)
            oldfile.close(self)
            #openfiles[ self.x ] = None
            del openfiles[self.x]

    oldopen = __builtin__.open

    def newopen(*args):
        if DEBUGFILES:
            from Ganga.Utility.logging import getLogger
            logger = getLogger()
            logger.debug("NewOpen")
        return newfile(*args)
    __builtin__.file = newfile
    __builtin__.open = newopen

    def printOpenFiles():
        from Ganga.Utility.logging import getLogger
        logger = getLogger()
        logger.debug("### %d OPEN FILES: [%s]" % (len(openfiles), ", ".join(
            str(f) for f in openfiles.keys() if openfiles[f] is not None)))

    safeFiles = ['.ganga.log', '.gangarc', 'ipythonrc', 'history', 'persist']

    def safeCloseOpenFiles():
        for f in openfiles.keys():
            if f not in safeFiles:
                openfiles[f].close()


def manualExportToGPI(my_interface=None):
    """ Additionally populate the GPI elements which are required
        This is here rather than in utility due to having knowledge of what is inside Ganga
        whilst Utility should not have too much (or any) of this
    """

    if not my_interface:
        import Ganga.GPI
        my_interface = Ganga.GPI

    from Ganga.Runtime.GPIexport import exportToInterface

    from Ganga.GPIDev.Base import ProtectedAttributeError, ReadOnlyObjectError, GangaAttributeError
    from Ganga.GPIDev.Lib.Job.Job import JobError

    exportToInterface(my_interface, 'GangaAttributeError', GangaAttributeError, 'Exceptions')
    exportToInterface(my_interface, 'ProtectedAttributeError', ProtectedAttributeError, 'Exceptions')
    exportToInterface(my_interface, 'ReadOnlyObjectError', ReadOnlyObjectError, 'Exceptions')
    exportToInterface(my_interface, 'JobError', JobError, 'Exceptions')

    from Ganga.Runtime.GPIFunctions import license, typename, categoryname, plugins, convert_merger_to_postprocessor

    exportToInterface(my_interface, 'license', license, 'Functions')
    # FIXME:
    #from Ganga.Runtime.GPIFunctions import applications, backends, list_plugins
    #exportToInterface(my_interface, 'applications', applications, 'Functions)
    #exportToInterface(my_interface, 'backends', backends, 'Functions')
    #exportToInterface(my_interface, 'list_plugins', list_plugins, 'Functions')
    # FIXME: END DEPRECATED

    exportToInterface(my_interface, 'typename', typename, 'Functions')
    exportToInterface(my_interface, 'categoryname', categoryname, 'Functions')
    exportToInterface(my_interface, 'plugins', plugins, 'Functions')
    exportToInterface(my_interface, 'convert_merger_to_postprocessor', convert_merger_to_postprocessor, 'Functions')

    from Ganga.GPIDev.Persistency import export, load
    exportToInterface(my_interface, 'load', load, 'Functions')
    exportToInterface(my_interface, 'export', export, 'Functions')



    from Ganga.GPIDev.Credentials import getCredential
    # only the available credentials are exported
    # At this point we expect to have the GridProxy already created
    # by one of the Grid plugins (LCG/NG/etc) so we search for it in creds
    # cache
    credential = getCredential(name='GridProxy', create=False)
    if credential:
        exportToInterface(my_interface, 'gridProxy', credential, 'Objects', 'Grid proxy management object.')

    credential2 = getCredential('AfsToken')
    if credential2:
        exportToInterface(my_interface, 'afsToken', credential2, 'Objects', 'AFS token management object.')

    # export full_print
    from Ganga.GPIDev.Base.VPrinter import full_print
    exportToInterface(my_interface, 'full_print', full_print, 'Functions')

    import Ganga.GPIDev.Lib.Config
    exportToInterface(my_interface, 'config', Ganga.GPIDev.Lib.Config.config, 'Objects', 'access to Ganga configuration')
    exportToInterface(my_interface, 'ConfigError', Ganga.GPIDev.Lib.Config.ConfigError, 'Exceptions')

    from Ganga.Utility.feedback_report import report
    exportToInterface(my_interface, 'report', report, 'Functions')

    from Ganga.Core.InternalServices.Coordinator import enableInternalServices, disableMonitoringService, enableMonitoringService, disableInternalServices
    exportToInterface(my_interface, 'reactivate', enableInternalServices, 'Functions')
    exportToInterface(my_interface, 'disableMonitoring', disableMonitoringService, 'Functions')
    exportToInterface(my_interface, 'enableMonitoring', enableMonitoringService, 'Functions')
    exportToInterface(my_interface, 'disableServices', disableInternalServices, 'Functions')

    from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
    exportToInterface(my_interface, 'GangaList', GangaList, 'Classes')

    from Ganga.GPIDev.Lib.Registry.JobRegistry import jobSlice
    exportToInterface(my_interface, "jobSlice", jobSlice, "Functions")

class GangaProgram(object):

    """ High level API to create instances of Ganga programs and configure/run it """

    def __init__(self, hello_string=None, argv=sys.argv):
        """ make an instance of Ganga program
        use default hello_string if not specified
        use sys.argv as arguments if not specified"""

        self.argv = argv[:]
        # record the start time.Currently we are using this in performance measurements
        # see Ganga/old_test/Performance tests
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

        self.default_config_file = os.path.expanduser('~/.gangarc')

        if sys.hexversion < 0x020700F0:
            version = '{0}.{1}'.format(sys.version_info[0], sys.version_info[1])
            from Ganga.Utility.logging import getLogger
            logger = getLogger()
            logger.warning('Ganga will soon be depending on Python 2.7. '
                           'You have Python {version} installed. '
                           'See https://github.com/ganga-devs/ganga/wiki/Python-2.7'.format(version=version))

    def exit(self, *msg):
        from Ganga.Utility.logging import getLogger
        logger = getLogger()
        logger.info(self.hello_string)
        for m in msg:
            logger.error(m)
        sys.exit(1)

    # parse the options

    def parseOptions(self):
        from optparse import OptionParser

        usage = self.hello_string + \
            """\nusage: %prog [options] [script] [args] ..."""

        parser = OptionParser(usage, version=_gangaVersion)

        parser.add_option("-i", dest="force_interactive", action="store_true",
                          help='enter interactive mode after running script')

        parser.add_option("--webgui", dest="webgui",  action="store_true", default='False',
                          help='starts web GUI monitoring server')

        parser.add_option("--config", dest="config_file", action="store", metavar="FILE", default=None,
                          help='read user configuration from FILE, overrides the GANGA_CONFIG_FILE environment variable. Default: ~/.gangarc')

        parser.add_option("--config-path", dest='config_path', action="store", default=None,
                          help='site/experiment config customization path, overrides the GANGA_CONFIG_PATH environment variable. The relative paths are resolved wrt to the release directory. To use a specific file you should specify the absolute path. Default: None')

        parser.add_option("-g", "--generate-config", dest='generate_config', action="store_const", const=1,
                          help='generate a default config file, backup the existing one')

        parser.add_option("-o", "--option", dest='cmdline_options', action="append", default=[], metavar='EXPR',
                          help='set configuration options, may be repeated mutiple times,'
                               'for example: -o[Logging]Ganga.Lib=DEBUG -oGangaLHCb=INFO -o[Configuration]TextShell = IPython '
                               'The VALUE of *_PATH option is prepended. To reset it use :::VALUE')

        parser.add_option("--quiet", dest="force_loglevel", action="store_const", const='ERROR',
                          help='only ERROR messages are printed')

        parser.add_option("--very-quiet", dest="force_loglevel", action="store_const", const='CRITICAL',
                          help='only CRITICAL messages are printed')

        parser.add_option("--debug", dest="force_loglevel", action="store_const", const='DEBUG',
                          help='all messages including DEBUG are printed')

        parser.add_option("--no-mon", dest='monitoring', action="store_const", const=0,
                          help='disable the monitoring loop (useful if you run multiple Ganga sessions)')

        parser.add_option("--no-prompt", dest='prompt', action="store_const", const=0,
                          help='never prompt interactively for anything except IPython (FIXME: this is not fully implemented)')

        parser.add_option("--no-rexec", dest="rexec", action="store_const", const=0,
                          help='rely on existing environment and do not re-exec ganga process'
                               'to setup runtime plugin modules (affects LD_LIBRARY_PATH)')

        parser.add_option("--test", dest='TEST', action="store_true", default=False,
                          help='run Ganga test(s) using internal test-runner. It requires GangaTest package to be installed.'
                               'Usage example: *ganga --test Ganga/old_test/MyTestcase* .'
                               'Refer to [TestingFramework] section in Ganga config for more information on how to configure the test runner.')

        parser.add_option("--daemon", dest='daemon', action="store_true", default=False,
                          help='run Ganga as service.')

        parser.set_defaults(force_interactive=False, config_file=None,
                            force_loglevel=None, rexec=1, monitoring=1, prompt=1, generate_config=None)
        parser.disable_interspersed_args()

        (self.options, self.args) = parser.parse_args(args=self.argv[1:])

        def file_opens(f, message):
            try:
                return open(f)
            except IOError as x:
                self.exit(message, x)

        if self.options.config_file == '':
            self.options.config_file = None
        self.options.config_file_set_explicitly = not self.options.config_file is None

        # use GANGA_CONFIG_FILE env var if it's set
        if self.options.config_file is None:
            self.options.config_file = os.environ.get(
                'GANGA_CONFIG_FILE', None)

        if self.options.config_file:
            import Ganga.Utility.files
            self.options.config_file = Ganga.Utility.files.expandfilename(
                self.options.config_file)
            open_file = file_opens(
                self.options.config_file, 'reading configuration file')
            open_file.close()
        # we run in the batch mode if a script has been specified and other
        # options (such as -i) do not force it
        if len(self.args) > 0:
            if not self.options.force_interactive:
                self.interactive = False

# Can't check here if the file is readable, because the path isn't known
#           file_opens(self.args[0],'reading script')

    @staticmethod
    def new_version():
        versions_filename = os.path.join(getConfig('Configuration')['gangadir'], '.used_versions')
        if not os.path.exists(versions_filename):
            with open(versions_filename, 'w') as versions_file:
                versions_file.write(_gangaVersion + '\n')
            return True

        with open(versions_filename, 'r+') as versions_file:
            if versions_file.read().find(_gangaVersion) < 0:
                versions_file.write(_gangaVersion + '\n')
                return True

        return False

    @staticmethod
    def rollHistoryForward():
        versions_filename = os.path.join(getConfig('Configuration')['gangadir'], '.used_versions')
        hasLoaded_newer = False
        with open(versions_filename, 'r') as versions_file:
            this_version = versions_file.read()
            split_version_info = this_version.split('.')
            if len(split_version_info) == 3:
                major = int(split_version_info[0])
                minor = int(split_version_info[1])
                debug = int(split_version_info[2])
                if major > 6:
                    hasLoaded_newer = True
                if major == 6 and minor > 1:
                    hasLoaded_newer = True
                if major == 6 and minor == 1 and debug > 13:
                    hasLoaded_newer = True
        
        old_dir = os.path.expanduser('~/.ipython-ganga')
        if 'IPYTHONDIR' in os.environ.keys():
            old_dir = os.path.abspath(os.path.expanduser(os.environ['IPYTHONDIR']))

        single_pass_file = os.path.join(old_dir, '.have_migrated')
        from Ganga.Utility.logging import getLogger
        logger = getLogger()
        logger.debug("testing: %s" % single_pass_file)

        if not os.path.exists(single_pass_file) and os.path.exists(os.path.join(old_dir, 'history')):

            logger.info("This is your first time running Ganga >=6.1.14")
            logger.info("Now attempting to migrate your IPython history file")

            try:
                from IPython.core.interactiveshell import InteractiveShell
                from IPython.core.history import HistoryManager
                from IPython.utils.path import locate_profile, get_ipython_dir
            except ImportError as err:
                logger.error("Unable to import required IPython files, can't roll forward history")
                logger.error("If you want to roll forward your old history files you'll need to do this by hand")
                logger.error("Please try running: https://gist.github.com/minrk/6003365")

            old_text_history = os.path.join(old_dir, 'history')
            new_sqlite_history = os.path.join(old_dir, "profile_default/history.sqlite")

            if not os.path.exists(os.path.join(old_dir, "profile_default")):
                os.makedirs(os.path.join(old_dir, "profile_default"))

            IPython_history = HistoryManager(hist_file=new_sqlite_history)#, shell = InteractiveShell.instance())

            with open(old_text_history) as hist_file:
                for linenum, line in enumerate(hist_file):
                    IPython_history.store_inputs(linenum, line)

            ## Set a file to indicate that we've already made this transition
            passed_file = open(single_pass_file, "w")
            passed_file.close()

            logger.info("IPython history migrated successfully.")


    @staticmethod
    def generate_config_file(config_file):
        from Ganga.GPIDev.Lib.Config.Config import config_file_as_text
        from Ganga.Utility.logging import getLogger
        logger = getLogger()

        # Old backup routine
        if os.path.exists(config_file):
            for i in range(100):
                bn = "%s.%.2d" % (config_file, i)
                if not os.path.exists(bn):
                    try:
                        os.rename(config_file, bn)
                    except Exception, err:
                        logger.error('Failed to create config backup file %s' % bn)
                        logger.error('Old file will not be overwritten, please manually remove it and start ganga with the -g option to re-generate it')
                        logger.error('Reason: %s' % err)
                        return
                    logger.info('Copied current config file to %s' % bn)
                    break
            else:
                config_directory = os.path.dirname(os.path.abspath(config_file))
                config_backupdir = os.path.join(config_directory, '.gangarc_backups')
                if not os.path.exists(config_backupdir):
                    os.makedirs(config_backupdir)

                datestr = "_" + time.strftime("%d.%m.%y")
                logger.info('Copying backup config files to %s' %
                            config_backupdir)
                for i in range(100):
                    old_bn = "%s.%.2d" % (config_file, i)
                    bn = os.path.basename(old_bn)
                    new_bn = bn + datestr
                    new_bn_file = os.path.join(config_backupdir, new_bn)
                    os.rename(old_bn, new_bn_file)
                bn = "%s.%.2d" % (config_file, 0)
                os.rename(config_file, bn)
                logger.info('Copied current config file to %s' % bn)
                #raise ValueError('too many bckup files')

        logger.info('Creating ganga config file %s' % config_file)
        new_config = ''

        with open(os.path.join(os.path.dirname(Ganga.Runtime.__file__), 'HEAD_CONFIG.INI'), 'r') as config_head_file:
            new_config += config_head_file.read()
        new_config += config_file_as_text()
        new_config = new_config.replace('Ganga-SVN', new_version_format_to_old(_gangaVersion))  #Add in Ganga-x-y-z format so that it is backward compatible.
        with open(config_file, 'w') as new_config_file:
            new_config_file.write(new_config)

    @staticmethod
    def print_release_notes():
        from Ganga.Utility.logging import getLogger
        from Ganga.Utility.Config.Config import getConfig
        import itertools
        logger = getLogger('ReleaseNotes')
        if getConfig('Configuration')['ReleaseNotes'] == True:
            packages = itertools.imap(lambda x: 'ganga/python/' + x, itertools.ifilter(lambda x: x != '', ['Ganga'] + getConfig('Configuration')['RUNTIME_PATH'].split(':')))
            pathname = os.path.join(os.path.dirname(__file__), '..', '..', '..', 'release', 'ReleaseNotes-%s' % _gangaVersion)

            if not os.path.exists(pathname):
                logger.warning("couldn't find release notes for version %s" % _gangaVersion)
                return

            bounding_line = '**************************************************************************************************************\n'
            dividing_line = '--------------------------------------------------------------------------------------------------------------\n'
            with open(pathname, 'r') as f:
                try:
                    notes = [l.strip() for l in f.read().replace(bounding_line, '').split(dividing_line)]
                except Exception, err:
                    logger.error('Error while attempting to read release notes')
                    logger.debug('Reason: %s' % err)
                    raise

            if notes[0].find(_gangaVersion) < 0:
                logger.error("Release notes version doesn't match the stated version on line 1")
                logger.error("'%s' does not match '%s'" % (_gangaVersion, notes[0]))
                return

            log_divider = '-' * 50
            note_gen = [(p, notes[notes.index(p) + 1].splitlines()) for p in packages if p in notes]
            if note_gen:
                logger.info(log_divider)
                logger.info(log_divider)
                logger.info("Release notes for version 'Ganga-%s':" % _gangaVersion)
                logger.info(log_divider)
                logger.info('')
                logger.info(log_divider)
                for p, n in note_gen:
                    logger.info(p)
                    logger.info(log_divider)
                    # logger.info('*'*len(p))
                    logger.info('')
                    for l in n:
                        logger.info(l.strip())
                    logger.info('')
                    logger.info(log_divider)
                logger.info(log_divider)

    # this is an option method which runs an interactive wizard which helps new users to start with Ganga
    # the interactive mode is not entered if -c option was used
    def new_user_wizard(self):
        from Ganga.Utility.logging import getLogger
        from Ganga.Utility.Config.Config import load_user_config, getConfig, ConfigError

        logger = getLogger('NewUserWizard')
        specified_gangadir = os.path.expanduser(os.path.expandvars(getConfig('Configuration')['gangadir']))
        specified_config = self.options.config_file
        default_gangadir = os.path.expanduser('~/gangadir')
        default_config = self.default_config_file

        if not os.path.exists(specified_gangadir) \
                and not os.path.exists(specified_config):
            logger.info('It seems that you run Ganga for the first time')
            logger.info('Ganga will send a udp packet each time you start it in order to help the development team understand how ganga is used. You can disable this in the config file by resetting [Configuration]UsageMonitoringURL=  ')

        if not os.path.exists(specified_gangadir) \
                and not os.path.exists(default_gangadir):
            logger.info('Making default gangadir: %s' % gangadir)
            try:
                os.makedirs(gangadir)
            except OSError as err:
                logger.error("Failed to create default gangadir '%s': %s" % (gangadir, err.message))
                raise
        if self.options.generate_config:
            logger = getLogger('ConfigUpdater')
            logger.info('re-reading in old config for updating...')
            load_user_config(specified_config, {})
            self.generate_config_file(specified_config)
            sys.exit(0)
        if not os.path.exists(specified_config) \
                and not os.path.exists(default_config):
            # Sleep for 1 sec to allow for most of the bootstrap to finish so
            # the user actually sees this message last
            time.sleep(3.)
            yes = raw_input('Would you like to create default config file ~/.gangarc with standard settings ([y]/n) ?\n')
            if yes == '' or yes[0:1].upper() == 'Y':
                self.generate_config_file(default_config)
                raw_input('Press <Enter> to continue.\n')
        elif self.new_version():
            self.print_release_notes()
            self.rollHistoryForward()
            # if config explicitly set we dont want to update the versions file
            # this is so that next time ganga used with the default .gangarc
            # it will still be classed as new so the default config is updated.
            # also if config explicitly set dont try to update it for new version
            # impacts hammercloud if you do?
            if not self.options.config_file_set_explicitly:
                logger = getLogger('ConfigUpdater')
                logger.info('It appears that this is the first time you have run %s' % _gangaVersion)
                logger.info('Your ganga config file will be updated.')
                logger.info('re-reading in old config for updating...')
                load_user_config(specified_config, {})
                self.generate_config_file(specified_config)

                # config file generation overwrites user values so we need to reapply the cmd line options to these user settings
                # e.g. set -o[Configuration]gangadir=/home/mws/mygangadir and the user value gets reset to the .gangarc value
                # (not the session value but the user value has precedence)
                try:
                    opts = self.parse_cmdline_config_options(self.options.cmdline_options)
                    for section, option, val in opts:
                        config = getConfig(section).setUserValue(option, val)
                except ConfigError as x:
                    self.exit('command line option error when resetting after config generation: %s' % x)

    @staticmethod
    def parse_cmdline_config_options(cmdline_options):
        """ Parse a list of command line config options and return a list of triplets (section,option,value).
        In case of parsing errors, raise ConfigError exception.
        """
        mpat = re.compile(r'(\[(?P<section>\S+)\]|)(?P<option>[a-zA-z0-9._/]+)=(?P<value>.+)')
        section = None

        opts = []
        for o in cmdline_options:
            rpat = mpat.match(o)
            if rpat is None:
                from Ganga.Utility.Config import ConfigError
                raise ConfigError('syntax error: "%s"' % o)
            else:
                if rpat.group('section'):
                    section = rpat.group('section')
                if section is None:
                    from Ganga.Utility.Config import ConfigError
                    raise ConfigError('section not specified: %s' % o)
                else:
                    opts.append((section, rpat.group('option'), rpat.group('value')))
        return opts

    # configuration procedure: read the configuration files, configure and
    # bootstrap logging subsystem
    def configure(self, logLevel=None):

        import Ganga.Utility.Config
        from Ganga.Utility.Config import ConfigError

        def set_cmdline_config_options(sects=None):
            try:
                opts = self.parse_cmdline_config_options(self.options.cmdline_options)
                for section, option, val in opts:
                    should_set = True
                    if not sects is None and not section in sects:
                        should_set = False
                    if should_set:
                        config = Ganga.Utility.Config.setSessionValue(section, option, val)
            except ConfigError as x:
                self.exit('command line option error: %s' % x)

        # set logging options
        set_cmdline_config_options(sects=['Logging'])

        # we will be reexecutig the process so for the moment just shut up
        # (unless DEBUG was forced with --debug)
        if self.options.rexec and 'GANGA_INTERNAL_PROCREEXEC' not in os.environ and not self.options.generate_config and 'GANGA_NEVER_REEXEC' not in os.environ:
            if self.options.force_loglevel != 'DEBUG':
                self.options.force_loglevel = 'CRITICAL'
        else:  # say hello
            if logLevel:
                self.options.force_loglevel = logLevel
            if self.options.force_loglevel in (None, 'DEBUG'):
                sys.stdout.write(str(self.hello_string)+'\n')

        if self.options.config_file is None or self.options.config_file == '':
            self.options.config_file = self.default_config_file

        # initialize logging for the initial phase of the bootstrap
        # will use the default, hardcoded log level in the module until
        # pre-configuration procedure is complete
        from Ganga.Utility.logging import force_global_level, getLogger

        force_global_level(self.options.force_loglevel)

        try:
            with open(self.options.config_file) as cf:
                first_line = cf.readline()
                r = re.compile('# Ganga configuration file \(\$[N]ame: (?P<version>\S+) \$\)').match(first_line)
                this_logger = getLogger("Configure")
                if not r:
                    this_logger.error('file %s does not seem to be a Ganga config file', self.options.config_file)
                    this_logger.error('try -g option to create valid ~/.gangarc')
                else:
                    cv = r.group('version').split('-')  #Version number is in Ganga-x-y-z format
                    if len(cv) == 1:
                        cv = new_version_format_to_old(cv[0]).split('-')
                    if cv[1] != '6':
                        this_logger.error('file %s was created by a development release (%s)', self.options.config_file, r.group('version'))
                        this_logger.error('try -g option to create valid ~/.gangarc')
        except IOError as x:
            # ignore all I/O errors (e.g. file does not exist), this is just an
            # advisory check
            this_logger = getLogger("Configure")
            this_logger.debug("Config File Exception: %s" % x)

        if self.options.config_path is None:
            try:
                self.options.config_path = os.environ['GANGA_CONFIG_PATH']
            except KeyError, err:
                self.options.config_path = ''
            if self.options.config_path is None:
                self.options.config_path = ''

        import Ganga.Utility.files
        import Ganga.Utility.util
        import inspect
        GangaRootPath = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), '../..'))

        if self.options.config_path:
            self.options.config_path = Ganga.Utility.files.expandfilename(os.path.join(GangaRootPath, self.options.config_path))

        # check if the specified config options are different from the defaults
        # and set session values appropriately
        syscfg = getConfig("System")
        if self.options.config_path != syscfg['GANGA_CONFIG_PATH']:
            syscfg.setSessionValue('GANGA_CONFIG_PATH', self.options.config_path)
        if self.options.config_file != syscfg['GANGA_CONFIG_FILE']:
            syscfg.setSessionValue('GANGA_CONFIG_FILE', self.options.config_file)

        def deny_modification(name, x):
            raise Ganga.Utility.Config.ConfigError(
                'Cannot modify [System] settings (attempted %s=%s)' % (name, x))
        syscfg.attachUserHandler(deny_modification, None)
        syscfg.attachSessionHandler(deny_modification, None)

        config = getConfig("Configuration")

        # detect default user (equal to unix user name)
        import getpass
        try:
            config.options['user'].default_value = getpass.getuser()
        except Exception as x:
            raise Ganga.Utility.Config.ConfigError('Cannot get default user name %s' % x)

        # import configuration from spyware
        from Ganga.Runtime import spyware

        monConfig = getConfig("MSGMS")

        # prevent modification during the interactive ganga session
        def deny_modification(name, x):
            raise Ganga.Utility.Config.ConfigError('Cannot modify [MSGMS] settings (attempted %s=%s)' % (name, x))
        monConfig.attachUserHandler(deny_modification, None)


        # all relative names in the path are resolved wrt the _gangaPythonPath
        # the list order is reversed so that A:B maintains the typical path precedence: A overrides B
        # because the user config file is put at the end it always may override
        # everything else
        config_files = Ganga.Utility.Config.expandConfigPath(self.options.config_path, _gangaPythonPath)
        config_files.reverse()

        # read-in config files

        # FIXME: need to construct a proper dictionary - cannot use the
        # ConfigPackage directly
        system_vars = {}
        for opt in syscfg:
            system_vars[opt] = syscfg[opt]

        def _createpath(dir):

            def _accept(fname, p=re.compile('.*\.ini$')):
                return (os.path.isfile(fname) or os.path.islink(fname)) and p.match(fname)
            files = []
            if dir and os.path.exists(dir) and os.path.isdir(dir):
                files = [os.path.join(dir, f) for f in os.listdir(dir) if
                         _accept(os.path.join(dir, f))]
            return string.join(files, os.pathsep)

        def _versionsort(s, p=re.compile(r'^(\d+)-(\d+)-*(\d*)')):
            m = p.match(s)
            if m:
                if m.group(3) == '':
                    return int(m.group(1)), int(m.group(2)), 0
                else:
                    return int(m.group(1)), int(m.group(2)), int(m.group(3))
            if s == 'SVN':
                return 'SVN'
            return None

        if "GANGA_SITE_CONFIG_AREA" in os.environ:
            this_dir = os.environ['GANGA_SITE_CONFIG_AREA']
            if os.path.exists(this_dir) and os.path.isdir(this_dir):
                dirlist = sorted(os.listdir(this_dir), key=_versionsort)
                dirlist.reverse()
                gangaver = _versionsort(new_version_format_to_old(_gangaVersion).lstrip('Ganga-')) #Site config system expects x-y-z version encoding
                for d in dirlist:
                    vsort = _versionsort(d)
                    if vsort and ((vsort <= gangaver) or (gangaver is 'SVN')):
                        select = os.path.join(this_dir, d)
                        config_files.append(_createpath(select))
                        break
        if os.path.exists(self.options.config_file):
            config_files.append(self.options.config_file)
        Ganga.Utility.Config.configure(config_files, system_vars)

        # set the system variables to the [System] module
        # syscfg.setDefaultOptions(system_vars,reset=1)

        # activate the logging subsystem
        # user defined log level takes effect NOW
        from Ganga.Utility.logging import bootstrap
        bootstrap()

        if not self.options.monitoring:
            self.options.cmdline_options.append('[PollThread]autostart=False')

        from Ganga.Utility.logging import getLogger

        logger = getLogger()

        logger.debug('default user name is %s', config['user'])
        logger.debug('user specified cmdline_options: %s', self.options.cmdline_options)

        # override the config options from the command line arguments
        # the format is [section]option=value OR option=value
        # in the second case last specified section from previous options is
        # used

        set_cmdline_config_options()

        if self.options.TEST:
            # FIXME: CONFIG CHECK
            # ?? config['RUNTIME_PATH'] = ''
            config.setSessionValue('RUNTIME_PATH', 'GangaTest')

        # ensure we're not interactive if daemonised
        if self.options.daemon and self.interactive:
            logger.warning("Cannot run as a service in interactive mode. Ignoring.")
            self.options.daemon = False

        # fork ourselves so we a daemon
        if self.options.daemon:
            logger.debug("Daemonising Ganga...")
            pid = os.fork()
            if pid < 0:
                sys.exit(1)

            if pid > 0:
                sys.exit(0)

            os.umask(0)
            os.setsid()
            pid = os.fork()

            if pid > 0:
                sys.exit(0)

            # change the stdout/err
            sys.stdout.flush()
            sys.stderr.flush()

            # create a server dir
            if not os.path.exists(os.path.join(config['gangadir'], "server")):
                os.makedirs(os.path.join(config['gangadir'], "server"))

            si = file("/dev/null", 'r')
            so = file(os.path.join(
                config['gangadir'], "server", "server-%s.stdout" % (os.uname()[1])), 'a')
            se = file(os.path.join(
                config['gangadir'], "server", "server-%s.stderr" % (os.uname()[1])), 'a', 0)

            os.dup2(si.fileno(), sys.stdin.fileno())
            os.dup2(so.fileno(), sys.stdout.fileno())
            os.dup2(se.fileno(), sys.stderr.fileno())

    # initialize environment: find all user-defined runtime modules and set their environments
    # if option rexec=1 then initEnvironment restarts the current ganga process (needed for LD_LIBRARY_PATH on linux)
    # set rexec=0 if you prepare your environment outside of Ganga and you do
    # not want to rexec process
    @staticmethod
    def initEnvironment(opt_rexec):

        from Ganga.Utility.logging import getLogger
        logger = getLogger()

        logger.debug("Installing Shutdown Manager")
        from Ganga.Core.InternalServices import ShutdownManager
        ShutdownManager.install()

        import Ganga.Utility.Config
        from Ganga.Utility.Runtime import RuntimePackage, allRuntimes
        from Ganga.Core import GangaException

        logger.debug("Import plugins")
        try:
            # load Ganga system plugins...
            from Ganga.Runtime import plugins
        except Exception as x:
            logger.critical('Ganga system plugins could not be loaded due to the following reason: %s', x)
            logger.exception(x)
            raise GangaException(x), None, sys.exc_info()[2]

        # initialize runtime packages, they are registered in allRuntimes
        # dictionary automatically
        try:
            import Ganga.Utility.files
            from Ganga.Utility.Config.Config import getConfig
            config = getConfig('Configuration')

            #if config['IgnoreRuntimeWarnings']:
            #    import warnings
            #    warnings.filterwarnings(action="ignore", category=RuntimeWarning)

            
            import inspect
            from os.path import expandvars, expanduser

            GangaRootPath = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))), '../..'))
            def transform(x):
                return os.path.normpath(Ganga.Utility.files.expandfilename(os.path.join(GangaRootPath,x)))

            paths = map(transform, filter(None, map(lambda x: expandvars(expanduser(x)), config['RUNTIME_PATH'].split(':'))))

            for path in paths:
                r = RuntimePackage(path)
        except KeyError, err:
            logger.debug("init KeyError: %s" % err)

        logger.debug("Internal_ProxReexec")

        # initialize the environment only if the current ganga process has not
        # been rexeced
        if 'GANGA_INTERNAL_PROCREEXEC' not in os.environ and 'GANGA_NEVER_REEXEC' not in os.environ:
            logger.debug('initializing runtime environment')
            # update environment of the current process
            for r in allRuntimes.values():
                try:
                    _env = r.getEnvironment()
                    if isinstance(_env, dict):
                        os.environ.update(_env)
                except Exception as err:
                    logger.error("can't get environment for %s, possible problem with the return value of getEvironment()" % r.name)
                    logger.error("Reason: %s" % err)
                    raise

            # in some cases the reexecution of the process is needed for LD_LIBRARY_PATH to take effect
            # re-exec the process if it is allowed in the options
            if opt_rexec:
                logger.debug('re-executing the process for LD_LIBRARY_PATH changes to take effect')
                os.environ['GANGA_INTERNAL_PROCREEXEC'] = '1'
                prog = os.path.normpath(sys.argv[0])
                logger.debug('Program: %s' % prog)
                logger.debug('sys.argv: %s' % sys.argv)
                os.execv(prog, sys.argv)

        else:
            logger.debug('skipped the environment initialization -- the processed has been re-execed and setup was done already')

        # bugfix 40110
        if 'GANGA_INTERNAL_PROCREEXEC' in os.environ:
            del os.environ['GANGA_INTERNAL_PROCREEXEC']

        from Ganga.Core.GangaThread.WorkerThreads import startUpQueues
        startUpQueues()

    # bootstrap all system and user-defined runtime modules
    @staticmethod
    def bootstrap(interactive):
        import Ganga.Utility.Config
        config = Ganga.Utility.Config.getConfig('Configuration')

        from Ganga.Utility.Runtime import loadPlugins, autoPopulateGPI
        loadPlugins(Ganga.GPI)
        autoPopulateGPI()
        from Ganga.Utility.Runtime import setPluginDefaults
        setPluginDefaults()

        # Start tracking all the threads and saving the information to a file
        stacktracer.trace_start()

        from Ganga.Utility.logging import getLogger
        logger = getLogger()

        manualExportToGPI()

        import Ganga.Core
        from Ganga.Runtime.Repository_runtime import startUpRegistries
        if config['AutoStartReg']:
            startUpRegistries()

        logger.debug("Bootstrap Core Modules")
        # bootstrap core modules
        from Ganga.Core.GangaRepository import getRegistrySlice
        ## Here is where the monitoring loop and related services are started!
        Ganga.Core.bootstrap(getRegistrySlice('jobs'), interactive)

        # export all configuration items, new options should not be added after
        # this point
        Ganga.GPIDev.Lib.Config.bootstrap()


        logger.debug("Post-Bootstrap hooks")
        from Ganga.Utility.Runtime import allRuntimes
        ###########
        # run post bootstrap hooks
        for r in allRuntimes.values():
            try:
                r.postBootstrapHook()
            except Exception as err:
                logger.error("problems with post bootstrap hook for %s" % r.name)
                logger.error("Reason: %s" % err)


    @staticmethod
    def startTestRunner(my_args):
        """
        run the testing framework
        """

        from Ganga.Utility.logging import getLogger
        logger = getLogger()

        try:
            # Important to avoid a lot of arguments over who has locked what object,
            # the tests are quite intensive and often trip on the background
            # monitoring thread(s)
            from Ganga.Core.InternalServices.Coordinator import disableMonitoringService
            disableMonitoringService()

            from GangaTest.Framework import runner
            from GangaTest.Framework import htmlizer
            from GangaTest.Framework import xmldifferencer

            tfconfig = Ganga.Utility.Config.getConfig('TestingFramework')
            rc = 1
            if tfconfig['EnableTestRunner']:
                logger.info("Starting Ganga Test Runner")

                if not my_args:
                    logger.warning("Please specify the tests to run ( i.e. ganga --test Ganga/old_test )")
                    return -1

                logger.info("myargs = %s" % " ".join(my_args))

                rc = runner.start(test_selection=" ".join(my_args))
            else:
                logger.info("Test Runner is disabled (set EnableTestRunner=True to enable it)")

            if rc > 0 and tfconfig['EnableHTMLReporter']:
                logger.info("Generating tests HTML reports")
                rc = htmlizer.main(tfconfig)
            elif rc > 0 and tfconfig['EnableXMLDifferencer']:
                logger.info("Generating difference HTML reports")
                rc = xmldifferencer.main(my_args)
            return rc
        except ImportError as e:
            logger.error("You need GangaTest external package in order to invoke Ganga test-runner.")
            logger.error(e)
            return -1

    # run Ganga in the specified namespace, in principle the namespace should import all names from Ganga.GPI
    # if namespace is not specified then run in __main__
    def run(self, local_ns=None):

        from Ganga.Utility.logging import getLogger
        logger = getLogger("run")
        logger.debug("Entering run")

        if self.options.webgui == True:
            from Ganga.Runtime.http_server import start_server
            start_server()

        if local_ns is None:
            import __main__
            local_ns = __main__.__dict__
        # save a reference to the Ganga namespace as an instance attribute
        self.local_ns = local_ns

        # load templates for user-defined runtime modules
        from Ganga.Utility.Runtime import allRuntimes
        for r in allRuntimes.values():
            r.loadTemplates(local_ns)

        # exec ~/.ganga.py file
        fileName = fullpath('~/.ganga.py')
        if os.path.exists(fileName):
            try:
                execfile(fileName, local_ns)
            except Exception as x:
                logger.error('Failed to source %s (Error was "%s"). Check your file for syntax errors.', fileName, x)
        # exec StartupGPI code
        from Ganga.Utility.Config import getConfig
        config = getConfig('Configuration')
        if config['StartupGPI']:
            # ConfigParser trims the lines and escape the space chars
            # so we have only one possibility to insert python code :
            # using explicitly '\n' and '\t' chars
            code = config['StartupGPI'].replace(
                '\\t', '\t').replace('\\n', '\n')
            exec code in local_ns

        logger.debug("loaded .ganga.py")

        # monitor the  ganga usage
        from Ganga.Runtime import spyware

        # this logic is a bit convoluted
        runs_script = len(self.args) > 0
        session_type = config['TextShell']
        if runs_script:
            if not self.interactive:
                session_type = 'batch'
                from Ganga.Utility.Config import setConfigOption
                setConfigOption('PollThread', 'forced_shutdown_policy', 'batch')
                from Ganga.Core import change_atexitPolicy
                change_atexitPolicy(False, 'batch')
            else:
                session_type += 'startup_script'

        spyware.ganga_started(session_type=session_type, interactive=self.interactive,
                              webgui=self.options.webgui,
                              script_file=runs_script, text_shell=config['TextShell'],
                              test_framework=self.options.TEST)

        if self.options.TEST:
            sys.argv = self.args
            try:
                rc = self.startTestRunner(self.args)
            except (KeyboardInterrupt, SystemExit):
                logger.warning('Test Runner interrupted!')
                import Ganga.Core.InternalServices.Coordinator
                if not Ganga.Core.InternalServices.Coordinator.servicesEnabled:
                    from Ganga.Core.InternalServices.Coordinator import enableInternalServices
                    enableInternalServices()
                sys.exit(1)
            sys.exit(rc)

        if len(self.args) > 0:
            # run the script and make it believe it that it is running directly
            # as an executable (sys.argv)
            saved_argv = sys.argv
            sys.argv = self.args

            import Ganga.Utility.Runtime
            path = Ganga.Utility.Runtime.getSearchPath()
            script = Ganga.Utility.Runtime.getScriptPath(self.args[0], path)

            #if script:
            #    script_file = open(script, 'r')
            #    script_content = script_file.read()
            #    script_file.close()
            #
            #    compiled_script = compile(script_content, '<string>', 'exec')
            #
            #exec compiled_script

            if script:
                execfile(script, local_ns)
            else:
                logger.error("'%s' not found" % self.args[0])
                logger.info("Searched in path %s" % path)
                sys.exit(1)
            sys.argv = saved_argv

        # and exit unless -i was specified
        if not self.interactive:
            return
            #    sys.exit(0)

        # interactive python shell

        shell = config['TextShell']

        if shell == 'IPython':

            try:
                import IPython
            except ImportError as err:
                logger.error("Cannot load IPython class!!!")
                logger.error("Please check your environment and re-load Ganga from a working shell!")
                logger.error("Exiting Ganga now, goodbye!")
                return

            import Ganga.Utility.Config
            ipconfig = Ganga.Utility.Config.getConfig('TextShell_IPython')
            args = eval(ipconfig['args'])

            # buffering of log messages from all threads called "GANGA_Update_Thread"
            # the logs are displayed at the next IPython prompt

            from Ganga.Utility.logging import enableCaching
            enableCaching()

            ipver = IPython.__version__

            ipver_major = int(ipver[0])
            ipver_minor = int(ipver[2])

            #if ipver in ["1.2.1", "3.1.0", "3.2.0", "3.2.1", '4.0.0']:
            if ipver_major > 1 or (ipver_major == 1 and ipver_minor >= 2):
                self.check_IPythonDir()
                self.launch_IPython(local_ns, args, self._ganga_error_handler, self.ganga_prompt)
            else:
                print("Unknown IPython version: %s" % ipver)
                return

        else:
            override_credits()
            import code
            sys.displayhook = _display
            c = code.InteractiveConsole(locals=local_ns)
            c.interact()

        return

    @staticmethod
    def check_IPythonDir():
        """
        Check that the .ipython folder exists as expected as defined by IPYTHONDIR
        """

        from Ganga.Utility.logging import getLogger
        logger = getLogger()

        not_exist = False

        if 'IPYTHONDIR' in os.environ.keys():
            ipyth_dir = os.environ['IPYTHONDIR']
            os.environ['IPYTHONDIR'] = os.path.abspath(os.path.expanduser(os.environ['IPYTHONDIR']))
            #logger.warning('Environment variable IPYTHONDIR=%s exists and overrides the default history file for Ganga IPython commands', ipyth_dir)
            if not os.path.isdir(ipyth_dir):
                if os.path.exists(ipyth_dir):
                    ipyth_dir_bak = str(ipyth_dir).append('_bak')
                    logger.warning('IPYTHONDIR points to: %s which is not a directory, moving it to: %s' % (ipyth_dir, ipyth_dir_bak))
                    os.rename(ipyth_dir, ipyth_dir_bak)
                    not_exist = True
        else:
            newpath = os.path.expanduser('~/.ipython-ganga')
            oldpath = os.path.expanduser('~/.ipython')
            os.environ['IPYTHONDIR'] = newpath
            if not os.path.exists(newpath):
                if os.path.exists(oldpath):
                    logger.warning('Default location of IPython history files has changed.')
                    logger.warning('Ganga will now try to copy your old settings from %s to the new path %s.\
                            If you do not want that, quit Ganga and wipe off the content of new path: rm -rf %s/*', oldpath, newpath, newpath)
                    import shutil
                    shutil.copytree(oldpath, newpath)
                else:
                    not_exist = True

        if not_exist:
            os.makedirs(os.environ['IPYTHONDIR'])

        rc_file = os.path.join(os.environ['IPYTHONDIR'], 'ipythonrc')
	logger.debug("Checking: %s" % rc_file)
        if not os.path.isfile(rc_file):
            lock = open(rc_file, "w")
            lock.close()

        return None

    @staticmethod
    def _ganga_error_handler(exception_obj, etype, value, tb, tb_offset=None):
        """
        Error handler for IPython 3.x+ to identify expected Ganga exceptions or unexpected uncaught exceptions from somewhere
        """
        ## see https://ipython.org/ipython-doc/dev/api/generated/IPython.core.interactiveshell.html#IPython.core.interactiveshell.InteractiveShell.set_custom_exc
        from Ganga.Utility.logging import getLogger
        logger = getLogger(modulename=True)
        logger.error("Error: %s" % value)

        from Ganga.Core.exceptions import GangaException
        if not issubclass(etype, GangaException):
            logger.error("Unknown/Unexpected ERROR!!")
            #logger.error("If you're able to reproduce this please report this to the Ganga developers!")
            #logger.error("value: %s" % value)
            exception_obj.showtraceback((etype, value, tb), tb_offset=tb_offset)
        return None

    @staticmethod
    def launch_IPython(local_ns, args, error_handler, ganga_prompt):
        """
        Launch an embedded IPython session within the Ganga.GPI namespace
        """

        import IPython

        # Based on examples/Embedding/embed_class_long.py from the IPython source tree

        # First we set up the prompt
        from IPython.config.loader import Config
        cfg = Config()
        cfg.TerminalInteractiveShell.colors = 'LightBG'
        cfg.TerminalInteractiveShell.autocall = 0
        cfg.PlainTextFormatter.pprint = True
        banner = exit_msg = ''
        prompt_config = cfg.PromptManager
        prompt_config.in_template = '[{time}]\nGanga In [\\#]: '
        prompt_config.in2_template = '         .\\D.: '
        prompt_config.out_template = 'Ganga Out [\\#]: '

        # Import the embed function
        from IPython.terminal.embed import InteractiveShellEmbed

        ## Check which version of IPython we're running
        ipver = IPython.__version__
        ipver_major = int(ipver[0])
        if ipver_major > 2:
            ipshell = InteractiveShellEmbed(argv=args, config=cfg, banner1=banner, exit_msg=exit_msg)
        else:
            ipshell = InteractiveShellEmbed(config=cfg, banner1=banner, exit_msg=exit_msg)

        ipshell.set_custom_exc((Exception,), error_handler)

        # buffering of log messages from all threads called "GANGA_Update_Thread"
        # the logs are displayed at the next IPython prompt
        from Ganga.Utility.logging import enableCaching
        enableCaching()

        ipshell.set_hook("pre_prompt_hook", ganga_prompt)

        from Ganga.Runtime.IPythonMagic import magic_ganga
        ipshell.define_magic('ganga', magic_ganga)

        from Ganga.Utility.Config.Config import getConfig
        config = getConfig('Configuration')

        ipshell.confirm_exit = config['confirm_exit']

        # Launch embedded shell
        from Ganga import GPI
        ipshell(local_ns=local_ns, module=Ganga.GPI)

    @staticmethod
    def ganga_prompt(dummy=None):
        from Ganga.Utility.logging import cached_screen_handler
        if cached_screen_handler:
            cached_screen_handler.flush()

        credentialsWarningPrompt = ''
        # alter the prompt only when the internal services are disabled
        from Ganga.Core.InternalServices import Coordinator
        if not Coordinator.servicesEnabled:
            invalidCreds = Coordinator.getMissingCredentials()
            if invalidCreds:
                credentialsWarningPrompt = '[%s required]' % ','.join(invalidCreds)
            if credentialsWarningPrompt:  # append newline
                 credentialsWarningPrompt += '\n'

        return credentialsWarningPrompt



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
