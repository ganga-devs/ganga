##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Executable.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
##########################################################################

from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem

from GangaCore.Utility.Config import getConfig

from GangaCore.GPIDev.Lib.File import ShareDir
from GangaCore.GPIDev.Lib.File.File import File
from GangaCore.Core.exceptions import ApplicationConfigurationError

from GangaCore.Utility.logging import getLogger

from GangaCore.GPIDev.Base.Proxy import getName, stripProxy

import os
import shutil
from GangaCore.Utility.files import expandfilename

logger = getLogger()

class Executable(IPrepareApp):

    """
    Executable application -- running arbitrary programs.

    When you want to run on a worker node an exact copy of your script you should specify it as a File object. Ganga will
    then ship it in a sandbox:
       app.exe = File('/path/to/my/script')

    When you want to execute a command on the worker node you should specify it as a string. Ganga will call the command
    with its full path on the worker node:
       app.exe = '/bin/date'

    A command string may be either an absolute path ('/bin/date') or a command name ('echo').
    Relative paths ('a/b') or directory paths ('/a/b/') are not allowed because they have no meaning
    on the worker node where the job executes.

    The arguments may be specified in the following way:
       app.args = ['-v',File('/some/input.dat')]

    This will yield the following shell command: executable -v input.dat
    The input.dat will be automatically added to the input sandbox.

    If only one argument is specified the the following abbreviation may be used:
       apps.args = '-v'

    """
    _schema = Schema(Version(2, 0), {
        'exe': SimpleItem(preparable=1, defvalue='echo', typelist=[str, File], comparable=1, doc='A path (string) or a File object specifying an executable.'),
        'args': SimpleItem(defvalue=["Hello World"], typelist=[str, File, int], sequence=1, strict_sequence=0, doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'env': SimpleItem(defvalue={}, typelist=[str], doc='Dictionary of environment variables that will be replaced in the running environment.'),
        'platform': SimpleItem(defvalue='ANY', typelist=[str], doc='Platform where the job will be executed, for example "x86_64-centos7-gcc8-opt"'),
        'is_prepared': SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=[None, ShareDir], protected=0, comparable=1, doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=[None, str], hidden=0, doc='MD5 hash of the string representation of applications preparable attributes')
    })
    _category = 'applications'
    _name = 'Executable'
    _exportmethods = ['prepare', 'unprepare']

    def __init__(self):
        super(Executable, self).__init__()

    def unprepare(self, force=False):
        """
        Revert an Executable() application back to it's unprepared state.
        """
        logger.debug('Running unprepare in Executable app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):
        """
        A method to place the Executable application into a prepared state.

        The application wil have a Shared Directory object created for it. 
        If the application's 'exe' attribute references a File() object, it 
        will be copied into the Shared Directory.

        Otherwise, if the 'exe' is a string, it is assumed that it is referencing 
        a file available in the user's path (as per the default "echo Hello World" 
        example). In this case, a wrapper script which calls this same command 
        is created and placed into the Shared Directory.

        When the application is submitted for execution, it is the contents of the
        Shared Directory that are shipped to the execution backend. 

        The Shared Directory contents can be queried with 
        shareref.ls('directory_name')

        See help(shareref) for further information.
        """

        if (self.is_prepared is not None) and not force:
            raise ApplicationPrepareError('%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))

        # lets use the same criteria as the configure() method for checking file existence & sanity
        # this will bail us out of prepare if there's somthing odd with the job config - like the executable
        # file is unspecified, has a space or is a relative path
        self.configure(self)
        logger.info('Preparing %s application.' % getName(self))
        self.is_prepared = ShareDir()
        logger.info('Created shared directory: %s' % (self.is_prepared.name))

        try:
            # copy any 'preparable' objects into the shared directory
            send_to_sharedir = self.copyPreparables()
            # add the newly created shared directory into the metadata system
            # if the app is associated with a persisted object
            self.checkPreparedHasParent(self)
            # return
            # [os.path.join(self.is_prepared.name,os.path.basename(send_to_sharedir))]
            self.post_prepare()

            if isinstance(self.exe, str):
                source = self.exe
            elif isinstance(self.exe, File):
                source = self.exe.name

            if isinstance(self.exe, str):
                logger.debug("exe is a string so no copying")
            elif not os.path.exists(source):
                logger.debug("Error copying exe: %s to input workspace" % str(source))
            else:
                try:
                    parent_job = self.getJobObject()
                except:
                    parent_job = None
                    pass
                if parent_job is not None:
                    input_dir = parent_job.getInputWorkspace(create=True).getPath()
                    shutil.copy2(source, input_dir)

        except Exception as err:
            logger.debug("Err: %s" % str(err))
            self.unprepare()
            raise

        return 1

    def configure(self, masterappconfig):
        from GangaCore.Core.exceptions import ApplicationConfigurationError
        import os.path

        # do the validation of input attributes, with additional checks for exe
        # property

        def validate_argument(x, exe=None):
            if isinstance(x, str):
                if exe:
                    if not x:
                        raise ApplicationConfigurationError('exe not specified')

                    if len(x.split()) > 1:
                        raise ApplicationConfigurationError('exe "%s" contains white spaces' % x)

                    dirn, filen = os.path.split(x)
                    if not filen:
                        raise ApplicationConfigurationError('exe "%s" is a directory' % x)
                    if dirn and not os.path.isabs(dirn) and self.is_prepared is None:
                        raise ApplicationConfigurationError('exe "%s" is a relative path' % x)
                    if not os.path.basename(x) == x:
                        if not os.path.isfile(x):
                            raise ApplicationConfigurationError('%s: file not found' % x)

            else:
                try:
                    # int arguments are allowed -> later converted to strings
                    if isinstance(x, int):
                        return
                    if not x.exists():
                        raise ApplicationConfigurationError('%s: file not found' % x.name)
                except AttributeError as err:
                    raise ApplicationConfigurationError('%s (%s): unsupported type, must be a string or File' % (str(x), str(type(x))))

        validate_argument(self.exe, exe=1)

        for a in self.args:
            validate_argument(a)

        return (None, None)

# disable type checking for 'exe' property (a workaround to assign File() objects)
# FIXME: a cleaner solution, which is integrated with type information in
# schemas should be used automatically
config = getConfig('defaults_Executable')  # _Properties
#config.setDefaultOption('exe',Executable._schema.getItem('exe')['defvalue'], type(None),override=True)
config.options['exe'].type = type(None)

# not needed anymore:
#   the backend is also required in the option name
#   so we need a kind of dynamic options (5.0)
#mc = getConfig('MonitoringServices')
#mc['Executable'] = None


def convertIntToStringArgs(args):

    result = []

    for arg in args:
        if isinstance(arg, int):
            result.append(str(arg))
        else:
            result.append(arg)

    return result


class RTHandler(IRuntimeHandler):

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        prepared_exe = app.exe
        if app.is_prepared is not None:
            shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']), 'shared', getConfig('Configuration')['user'])
            if isinstance(app.exe, str):
                prepared_exe = app.exe
            elif isinstance(app.exe, File):
                logger.info("Submitting a prepared application; taking any input files from %s" % (app.is_prepared.name))
                prepared_exe = File(os.path.join(
                    os.path.join(shared_path, app.is_prepared.name), os.path.basename(app.exe.name)))

        c = StandardJobConfig(prepared_exe, stripProxy(app).getJobObject().inputsandbox, convertIntToStringArgs(app.args), stripProxy(app).getJobObject().outputsandbox, app.env)
        return c


class LCGRTHandler(IRuntimeHandler):

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        from GangaCore.Lib.LCG import LCGJobConfig

        prepared_exe = app.exe
        if app.is_prepared is not None:
            shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                       'shared', getConfig('Configuration')['user'])
            if isinstance(app.exe, str):
                prepared_exe = app.exe
            elif isinstance(app.exe, File):
                logger.info("Submitting a prepared application; taking any input files from %s" % (
                    app.is_prepared.name))
                prepared_exe = File(os.path.join(
                    os.path.join(shared_path, app.is_prepared.name), os.path.basename(app.exe.name)))

        return LCGJobConfig(prepared_exe, app._getParent().inputsandbox, convertIntToStringArgs(app.args), app._getParent().outputsandbox, app.env)


class gLiteRTHandler(IRuntimeHandler):

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):
        from GangaCore.Lib.gLite import gLiteJobConfig

        prepared_exe = app.exe
        if app.is_prepared is not None:
            shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                       'shared', getConfig('Configuration')['user'])
            if isinstance(app.exe, str):
                prepared_exe = app.exe
            elif isinstance(app.exe, File):
                logger.info("Submitting a prepared application; taking any input files from %s" % (
                    app.is_prepared.name))
                prepared_exe = File(os.path.join(os.path.join(
                    shared_path, app.is_prepared.name), os.path.basename(File(app.exe).name)))

        return gLiteJobConfig(prepared_exe, app._getParent().inputsandbox, convertIntToStringArgs(app.args), app._getParent().outputsandbox, app.env)
from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('Executable', 'LSF', RTHandler)
allHandlers.add('Executable', 'Local', RTHandler)
allHandlers.add('Executable', 'PBS', RTHandler)
allHandlers.add('Executable', 'SGE', RTHandler)
allHandlers.add('Executable', 'Condor', RTHandler)
allHandlers.add('Executable', 'LCG', LCGRTHandler)
allHandlers.add('Executable', 'gLite', gLiteRTHandler)
allHandlers.add('Executable', 'TestSubmitter', RTHandler)
allHandlers.add('Executable', 'Interactive', RTHandler)
allHandlers.add('Executable', 'Batch', RTHandler)
allHandlers.add('Executable', 'Cronus', RTHandler)
allHandlers.add('Executable', 'Remote', LCGRTHandler)
allHandlers.add('Executable', 'CREAM', LCGRTHandler)
allHandlers.add('Executable', 'ARC', LCGRTHandler)
allHandlers.add('Executable', 'Slurm', RTHandler)
