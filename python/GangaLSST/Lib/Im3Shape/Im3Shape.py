##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Executable.py,v 1.1 2008-07-17 16:40:57 moscicki Exp $
##########################################################################

from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, GangaFileItem

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import File, ShareDir
from Ganga.Core import ApplicationConfigurationError, ApplicationPrepareError

from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Base.Proxy import getName, isType, stripProxy

import os
import shutil
from Ganga.Utility.files import expandfilename

from GangaDirac.Lib.Files import DiracFile
from Ganga.GPIDev.Lib.File import LocalFile

logger = getLogger()

class Im3Shape(IPrepareApp):

    """

    """
    _schema = Schema(Version(2, 0), {
        'location': GangaFileItem(defvalue=DiracFile(lfn='/lhcb/user/r/rcurrie/firstTestDiracFile.txt'), doc="Location of the Im3Shape program tarball"),
        'ini_location': GangaFileItem(defvalue=LocalFile('myIniFile.ini'), doc=".ini file used to configure Im3Shape"),
        'env': SimpleItem(defvalue={}, typelist=['str'], doc='Dictionary of environment variables that will be replaced in the running environment.'),
        'is_prepared': SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=['type(None)', 'bool', ShareDir], protected=0, comparable=1, doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=['type(None)', 'str'], hidden=0, doc='MD5 hash of the string representation of applications preparable attributes'),
        'blacklist': GangaFileItem(defvalue=DiracFile('/lhcb/user/r/rcurrie/secondTestDiracFile.txt'), doc="Blacklist file for running Im3Shape"),
        'rank': SimpleItem(defvalue=1, doc="Rank in the split of the tile from splitting"),
        'size': SimpleItem(defvalue=5, doc="Size of the splitting of the tile from splitting"),
        'catalog': GangaFileItem(defvalue=None, doc="Catalog which is used to describe what is processed"),
    })
    _category = 'applications'
    _name = 'Im3Shape'
    _exportmethods = ['prepare', 'unprepare']

    def __init__(self):
        super(Im3Shape, self).__init__()

    def __deepcopy__(self, memo):
        return super(Im3Shape, self).__deepcopy__(memo)

    def unprepare(self, force=False):
        """
        Revert an Executable() application back to it's unprepared state.
        """
        logger.debug('Running unprepare in Executable app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):
        """
        A method to place the Executable application into a prepared state.

        The application wil have a Shared Directory object created for it. 
        If the application's 'exe' attribute references a File() object or
        is a string equivalent to the absolute path of a file, the file 
        will be copied into the Shared Directory.

        Otherwise, it is assumed that the 'exe' attribute is referencing a 
        file available in the user's path (as per the default "echo Hello World"
        example). In this case, a wrapper script which calls this same command 
        is created and placed into the Shared Directory.

        When the application is submitted for execution, it is the contents of the
        Shared Directory that are shipped to the execution backend. 

        The Shared Directory contents can be queried with 
        shareref.ls('directory_name')

        See help(shareref) for further information.
        """

        if (self.is_prepared is not None) and (force is not True):
            raise ApplicationPrepareError('%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))

        # lets use the same criteria as the configure() method for checking file existence & sanity
        # this will bail us out of prepare if there's somthing odd with the job config - like the executable
        # file is unspecified, has a space or is a relative path
        self.configure(self)
        logger.info('Preparing %s application.' % getName(self))
        setattr(self, 'is_prepared', ShareDir())
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

           # if isType(self.exe, File):
           #     source = self.exe.name
           # elif isType(self.exe, str):
           #     source = self.exe
            
            #if not os.path.exists(source):
            #if False:#
            #    logger.debug("Error copying exe: %s to input workspace" % str(source))
            #else:
            #    try:
            #        parent_job = self.getJobObject()
            #    except:
            #        parent_job = None
            #        pass
            #    if parent_job is not None:
            #        input_dir = parent_job.getInputWorkspace(create=True).getPath()
            #        shutil.copy2(source, input_dir)

        except Exception as err:
            logger.debug("Err: %s" % str(err))
            self.unprepare()
            raise err

        return 1

    def configure(self, masterappconfig):
        from Ganga.Core import ApplicationConfigurationError
        import os.path

        # do the validation of input attributes, with additional checks for exe
        # property

        def validate_argument(x, exe=None):
            if isinstance(x, str):
                if exe:
                    if not x:
                        raise ApplicationConfigurationError(None, 'exe not specified')

                    if len(x.split()) > 1:
                        raise ApplicationConfigurationError(None, 'exe "%s" contains white spaces' % x)

                    dirn, filen = os.path.split(x)
                    if not filen:
                        raise ApplicationConfigurationError(None, 'exe "%s" is a directory' % x)
                    if dirn and not os.path.isabs(dirn) and self.is_prepared is None:
                        raise ApplicationConfigurationError(None, 'exe "%s" is a relative path' % x)
                    if not os.path.basename(x) == x:
                        if not os.path.isfile(x):
                            raise ApplicationConfigurationError(None, '%s: file not found' % x)

            else:
                try:
                    # int arguments are allowed -> later converted to strings
                    if isinstance(x, int):
                        return
                    if not x.exists():
                        raise ApplicationConfigurationError(None, '%s: file not found' % x.name)
                except AttributeError as err:
                    raise ApplicationConfigurationError(err, '%s (%s): unsupported type, must be a string or File' % (str(x), str(type(x))))

        #validate_argument(self.exe, exe=1)

        #for a in self.args:
        #    validate_argument(a)

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


