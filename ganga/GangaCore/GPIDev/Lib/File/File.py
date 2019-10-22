##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: File.py,v 1.2 2008-09-09 14:37:16 moscicki Exp $
##########################################################################

from GangaCore.Core.exceptions import GangaException, SchemaError
from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, GangaFileItem
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.GPIDev.Base.Proxy import stripProxy, GPIProxyObjectFactory
from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
from GangaCore.Core.GangaRepository.VStreamer import to_file, from_file
from GangaCore.Core.GangaRepository.GangaRepositoryXML import safe_save
from GangaCore.GPIDev.Base.Objects import synchronised
import os
import shutil
import uuid

from GangaCore.Utility.files import expandfilename, chmod_executable, is_executable

from GangaCore.Utility.logging import getLogger

from GangaCore.GPIDev.Base.Filters import allComponentFilters

import re

import GangaCore.Utility.Config

from GangaCore.GPIDev.Lib.File import getSharedPath

from GangaCore.Runtime.GPIexport import exportToGPI

import threading

# regex [[PROTOCOL:][SETYPE:]..[<alfanumeric>:][/]]/filename
urlprefix = re.compile(r'^(([a-zA-Z_][\w]*:)+/?)?/')

logger = getLogger()

_prepare_lock = threading.RLock()

class File(GangaObject):

    """Represent the files, both local and remote and provide an interface to transparently get access to them.

    Typically in the context of job submission, the files are copied to the directory where the application
    runs on the worker node. The 'subdir' attribute influances the destination directory. The 'subdir' feature
    is not universally supported however and needs a review.

    """
    _schema = Schema(Version(1, 1), {'name': SimpleItem(defvalue="", doc='path to the file source'),
                                     'subdir': SimpleItem(defvalue=os.curdir, doc='destination subdirectory (a relative path)'),
                                     'executable': SimpleItem(defvalue=False, hidden=True, transient=True, doc='specify if executable bit should be set when the file is created (internal framework use)')})
    _category = 'files'
    _name = "File"
    _exportmethods = ["getPathInSandbox", "exists", "create", "isExecutable"]

    # added a subdirectory to the File object. The default is os.curdir, that is "." in Unix.
    # The subdir is a relative path and will be appended to the pathname when writing out files.
    # Therefore changing subdir to a anything starting with "/" will still end up relative
    # to the pathname when the file is copied.
    #
    # There is no protection on putting the parent directory. So ".." is legal and will make
    # the file end up in the parent directory. - AM
    def __init__(self, name=None, subdir=os.curdir):
        super(File, self).__init__()

        if not name is None:
            try:
                assert(isinstance(name, str))
            except AssertionError:
                raise SchemaError("Name attribute should be a string type. Instead received: %s" % type(name))
            self.name = name

        if not subdir is None:
            self.subdir = subdir

    def __lt__(self, other):
        return len(self.name) < len(other.name)

    def __gt__(self, other):
        return len(self.name) > len(other.name)

    def __setattr__(self, attr, value):
        """
        This is an overloaded setter method to make sure that we're auto-expanding the filenames of files which exist.
        In the case we're assigning any other attributes the value is simply passed through
        Args:
            attr (str): This is the name of the attribute which we're assigning
            value (unknown): This is the value being assigned.
        """
        actual_value = value
        if attr == "name":
            actual_value = expandfilename(value)
        super(File, self).__setattr__(attr, actual_value)


    def _attribute_filter__set__(self, attribName, attribValue):
        if attribName is 'name':
            return expandfilename(attribValue)
        return attribValue

    def getPathInSandbox(self):
        """return a relative location of a file in a sandbox: subdir/name"""
        from GangaCore.Utility.files import real_basename
        return os.path.join(self.subdir, real_basename(self.name))

    def exists(self):
        """check if the file exists (as specified by 'name')"""
        import os.path
        return os.path.isfile(expandfilename(self.name))

    def create(self, outname):
        """create a file in  a local filesystem as 'outname', maintain
        the original permissions """
        import shutil

        shutil.copy(expandfilename(self.name), outname)
        if self.executable:
            chmod_executable(outname)

    def __repr__(self):
        """Get   the  representation   of  the   file.  Since   the  a
        SimpleStreamer uses  __repr__ for persistency  it is important
        to return  a valid python expression  which fully reconstructs
        the object.  """

        return "File(name='%s',subdir='%s')" % (self.name, self.subdir)

    def isExecutable(self):
        """  return true  if  a file  is  create()'ed with  executable
        permissions,  i.e. the  permissions of  the  existing 'source'
        file are checked"""
        return self.executable or is_executable(expandfilename(self.name))

# add File objects to the configuration scope (i.e. it will be possible to
# write instatiate File() objects via config file)
GangaCore.Utility.Config.config_scope['File'] = File

def string_file_shortcut_file(v, item):
    if isinstance(v, str):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        return File(v)
    return None

allComponentFilters['files'] = string_file_shortcut_file

class ShareDir(GangaObject):

    """Represents the directory used to store resources that are shared amongst multiple Ganga objects.

    Currently this is only used in the context of the prepare() method for certain applications, such as
    the Executable() application. A single ("prepared") application can be associated to multiple jobs.

    """
    _schema = Schema(Version(1, 0), {'name': SimpleItem(defvalue='', getter="_getName", doc='path to the file source'),
                                     'subdir': SimpleItem(defvalue=os.curdir, doc='destination subdirectory (a relative path)'),
                                     'associated_files': GangaFileItem(defvalue=[], typelist = [str, IGangaFile], doc='A list of files associated with the sharedir')})

    _category = 'shareddirs'
    _exportmethods = ['add', 'ls', 'path', 'remove', 'addAssociatedFile', 'listAssociatedFiles']
    _name = "ShareDir"

    def __init__(self, name=None, subdir=os.curdir):
        super(ShareDir, self).__init__()

        self._setRegistry(None)

        self._real_name = None

    def setSchemaAttribute(self, name, value):
        """
        This method intercepts the setter method used in reading in an XML file.
        This should be regarded as s specialist use case of overloading this method as
        this class plays fast and lose with the use of a getter for an attribute which is stored in memory.
        It may be worth reconsidering the use of a better getter method here if possible but unlikely as
        this would likely involve coding the class with implicit knowledge of it's proxy which is worse.
        If the Schema properly supported setters to match the getters this would be a much simplified structure.

        Args:
            name (str): Atribute being set
            value (unknown): value being assigned to that attribute
        """
        if name == "name":
            self._real_name = value
        else:
            # 'name' hidden behind setter here so shouldn't attempt to modify it
            super(ShareDir, self).setSchemaAttribute(name, value)

    def __setattr__(self, name, value):
        """
        A setattr wrapper which intercepts calls to assign self.name to self._real_name so the name gettter works
        Args:
            name (str): Name of the attribute being set
            value (unknown): value being assigned to the attribute
        """
        if name == 'name':
            self._real_name = value
        else:
            # 'name' hidden behind setter here so shouldn't attempt to modify it
            super(ShareDir, self).__setattr__(name, value)

    def __getattr__(self, name):
        """
        A getter wrapper which intercepts bare calls to name and redirects them to _real_name
        Args:
            name (str): The attribute which is being looked for
        """
        if name == 'name':
            return _getName()
        return super(ShareDir, self).__getattr__(name)

    def _getName(self):
        """
        A getter method for the 'name' schema attribute which will trigger the creation of a SharedDir on disk only when information about it is asked
        """

        with _prepare_lock:
            if not self._real_name:
                self._real_name = 'conf-{0}'.format(uuid.uuid4())
                share_dir = os.path.join(getSharedPath(), self._real_name)
                if not os.path.isdir(share_dir):
                    logger.debug("Actually creating: %s" % share_dir)
                    os.makedirs(share_dir)
                if not os.path.isdir(share_dir):
                    logger.error("ERROR creating path: %s" % share_dir)
                    raise GangaException("ShareDir ERROR")

        return self._real_name

    def add(self, input):
        from GangaCore.Core.GangaRepository import getRegistry
        if not isType(input, list):
            input = [input]
        for item in input:
            if isType(item, str):
                if os.path.isfile(expandfilename(item)):
                    logger.info('Copying file %s to shared directory %s' % (item, self.name))
                    shutil.copy2(expandfilename(item), os.path.join(getSharedPath(), self.name))
                    shareref = getRegistry("prep").getShareRef()
                    shareref.increase(self)
                    shareref.decrease(self)
                else:
                    logger.error('File %s not found' % expandfilename(item))
            elif isType(item, File) and item.name is not '' and os.path.isfile(expandfilename(item.name)):
                logger.info('Copying file object %s to shared directory %s' % (item.name, self.name))
                shutil.copy2(expandfilename(item.name), os.path.join(getSharedPath(), self.name))
                shareref = getRegistry("prep").getShareRef()
                shareref.increase(self)
                shareref.decrease(self)
            else:
                logger.error('File %s not found' % expandfilename(item.name))

    def path(self):
        """Get the full path of the ShareDir location"""
        return os.path.join(getSharedPath(), self._getName())
                
    def ls(self):
        """
        Print the contents of the ShareDir
        """
        full_shareddir_path = self.path()
        try:
            os.path.isdir(full_shareddir_path)
            cmd = "find '%s'" % (full_shareddir_path)
            files = os.popen(cmd).read().strip().split('\n')
            padding = '|  '
            for file in files:
                level = file.count(os.sep)
                level = level - 6
                pieces = file.split(os.sep)
                symbol = {0: '', 1: '/'}[os.path.isdir(file)]
                logger.info(padding * level + pieces[-1] + symbol)
        except IOError:
            logger.warn('ShareDir %s not found on storage' %
                        full_shareddir_path)

    def exists(self):
        """check if the file exists (as specified by 'name')"""
        import os.path
        return os.path.isdir(expandfilename(self.name))

    def create(self, outname):
        """create a file in  a local filesystem as 'outname', maintain
        the original permissions """
        import shutil

        shutil.copy(expandfilename(self.name), outname)
        if self.executable:
            chmod_executable(outname)

    def __repr__(self):
        """Get   the  representation   of  the   file.  Since   the  a
        SimpleStreamer uses  __repr__ for persistency  it is important
        to return  a valid python expression  which fully reconstructs
        the object.  """

        return "ShareDir(name='%s',subdir='%s')" % (self.name, self.subdir)

    def isExecutable(self):
        """  return true  if  a file  is  create()'ed with  executable
        permissions,  i.e. the  permissions of  the  existing 'source'
        file are checked"""
        return self.executable or is_executable(expandfilename(self.name))

    @synchronised
    def getAssociatedFiles(self):
        """ Load the list of associated files from the saved XML. This is
        necessary to keep the list consistent when copying jobs. """
        if os.path.isfile(os.path.join(self.path(), 'associated_files.xml')):
            with open(os.path.join(self.path(), 'associated_files.xml'), "r") as fobj:
                tmpobj, errs = from_file(fobj)
                self.associated_files = tmpobj

    @synchronised 
    def addAssociatedFile(self, newFile):
        """ Add an associated file to the ShareDir. Use this method to save
        it to theXML """
        self.getAssociatedFiles()
        self.associated_files.append(newFile)
        safe_save(os.path.join(self.path(), 'associated_files.xml'), self.associated_files, to_file)

    @synchronised
    def removeAssociatedFiles(self):
        """ Remove the files in the associated file list"""
        self.getAssociatedFiles()
        for entry in list(self.associated_files):
            if isinstance(entry, IGangaFile):
                entry.remove()
        self.associated_files = None

    @synchronised
    def listAssociatedFiles(self):
        """ List the associated_files of the ShareDir """
        self.getAssociatedFiles()
        return self.associated_files

    def remove(self):
        """ Remove the ShareDir and all of its associated files.
            This doesn't take care of the ShareRef so should only be
            called from there """
        shutil.rmtree(self.path(), ignore_errors=True)
        logger.info("Removed: %s" % self.path())
        self.removeAssociatedFiles()

GangaCore.Utility.Config.config_scope['ShareDir'] = ShareDir

def string_sharedfile_shortcut(v, item):
    if isinstance(v, str):
        # use proxy class to enable all user conversions on the value itself
        # but return the implementation object (not proxy)
        return ShareDir(v)
    return None

allComponentFilters['shareddirs'] = string_sharedfile_shortcut


def cleanUpShareDirs():
    """Function to be used to clean up erronious empty folders in the Shared directory"""
    share_path = getSharedPath()

    logger.info("Cleaning Shared folders in: %s" % share_path)
    logger.info("This may take a few minutes if you're running this for the first time after 6.1.23, feel free to go grab a tea/coffee")

    for item in os.listdir(share_path):
        this_dir = os.path.join(share_path, item)
        if os.path.isdir(this_dir):
            # NB we do need to explicitly test the length of the returned value here
            # Checking is __MUCH__ faster than trying and failing to remove folders with contents on AFS
            if len(os.listdir(this_dir)) == 0:
                try:
                    os.rmdir(this_dir)
                except OSError:
                    logger.debug("Failed to remove: %s" % this_dir)

exportToGPI('cleanUpShareDirs', cleanUpShareDirs, 'Functions')

#
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2008/07/17 16:40:53  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.15  2007/08/24 15:55:03  moscicki
# added executable flag to the file, ganga will set the executable mode of the app.exe file (in the sandbox only, the original file is not touched), this is to solve feature request #24452
#
# Revision 1.14  2007/01/25 16:18:21  moscicki
# mergefrom_Ganga-4-2-2-bugfix-branch_25Jan07 (GangaBase-4-14)
#
# Revision 1.12.2.4  2006/11/27 09:30:10  amuraru
# Extended the absolute paths detection in File objects
#
# Revision 1.12.2.3  2006/10/27 15:34:59  amuraru
# *** empty log message ***
#
# Revision 1.12.2.2  2006/10/27 15:33:59  amuraru
# bugs #20545
#
# Revision 1.12  2006/08/29 12:45:08  moscicki
# automatic conversion to absolute path
#
# Revision 1.11  2006/07/27 20:10:45  moscicki
# File to the configuartion symbol scope
#
# Revision 1.10  2006/02/17 14:21:32  moscicki
# fixed exists() method: using expanded file name
#
# Revision 1.9  2006/02/10 14:21:56  moscicki
# added exists() method
# docstring cleanup
#
# Revision 1.8  2005/08/23 17:07:23  moscicki
# Added executable flag for FileBuffer.
# Added create method for File and FileBuffer.
# Added getPathInSandbox() method.
#
# Revision 1.7  2005/08/10 09:45:36  andrew
# Added a subdir to File and FileBuffer objects. Changed the writefile method
# in FileWorspace to use the subdirectory
#
#
#
