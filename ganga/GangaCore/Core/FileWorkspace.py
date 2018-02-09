##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: FileWorkspace.py,v 1.2 2009-05-20 09:23:46 moscicki Exp $
##########################################################################

"""
FileWorkspace subsystem (aka LSE, SE,...) defines the interface to
create, copy and upload files which are part of Job definition such as
input  and  output sandboxes.  This  allows  to  handle files  in  the
FileWorkspace in a location-independent way.
"""

from GangaCore.Utility.logging import getLogger

import os
import time

from GangaCore.Utility.files import expandfilename, chmod_executable
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.Core.exceptions import GangaTypeError

logger = getLogger(modulename=1)

class FileWorkspace(object):

    """
    File workspace  on a local file system.   FileWorkspace object may
    represent any  part of the directory tree  (including 'top' i.e.
    root of the workspace).

    Files may be stored on  a 'per job' basis (optional jobid argument
    to create  method). A subpath may  specify a specific  part of the
    file workspace  (such as input/output sandbox). Note  that you may
    use  FileWorkspace to  store files  which are  not related  to any
    partical job (or are shared between jobs).

    The general directory layout :
     getPath() resolves to 'top/jobid/subpath/*'

    If  jobid  is  None  then  FileWorkspace  represents  the  topmost
    directory (with a given subpath  which may be an empty string ''),
    i.e.: getPath() resolves to 'top/subpath/*' or 'top/*' """


    __slots__=('jobid', 'top', 'subpath', '__removeTrials')

    def __init__(self, top, subpath=''):
        self.jobid = None
        self.top = top
        self.subpath = subpath
        self.__removeTrials = 0

    def create(self, jobid=None):
        """ create a workspace, an optional jobid parameter specifies the job directory
            you can call create() as many times as you want without any harm """

        # FIXME: make a helper method for os.makedirs
        logger.debug('creating %s', self.getPath())
        self.jobid = jobid
        try:
            import os.path
            if os.path.isdir(self.getPath()):
                return

            os.makedirs(self.getPath())
        except OSError as x:
            import errno
            if x.errno == errno.EEXIST:
                logger.debug('EEXIT: %s', self.getPath())
            else:
                raise

    # resolve a path to the filename in the context of the file workspace
    # if filename is None then return the directory corresponding to this file
    # workspace
    def getPath(self, filename=None):
        subpath = self.subpath
        if filename is None:
            filename = ''
        if self.jobid is not None:
            jobdir = str(self.jobid)
        else:
            jobdir = ''
            subpath = ''

        return expandfilename(os.path.join(self.top, jobdir, subpath, filename), True)

    # write a file (represent as file object) to the workspace
    # file object may be:
    #  - a File instance - referes to an existing file which will be copied to workspace directory
    #  - a FileBuffer instance - refers to a file which is does not yet exist but which contents is available in a memory buffer
    #      this is a handy way of creating wrapper scripts etc.
    #  - a tuple (name,contents) - deprecated - equivalent to FileBuffer
    #
    # File classes are define in GangaCore.GPIDev.Lib.File package
    #
    def writefile(self, fileobj, executable=None):

        from GangaCore.GPIDev.Lib.File import FileBuffer

        if not isType(fileobj, FileBuffer):
            raise GangaTypeError('Usage of tuples is not allowed, use FileBuffer instead')

        # output file name
        # Added a subdir to files, (see Ganga/GPIDev/Lib/File/File.py) This allows
        # to copy files into the a subdirectory of the workspace

        # FIXME: make a helper method for os.makedirs
        path_to_build = os.path.join(self.getPath(), fileobj.subdir)
        if not os.path.isdir(path_to_build):
            os.makedirs(path_to_build)
            logger.debug('created %s', self.getPath())
        else:
            logger.debug('already exists: %s', self.getPath())

        outname = expandfilename(self.getPath(fileobj.getPathInSandbox()), True)

        fileobj.create(outname)

        if executable:
            chmod_executable(outname)

        return outname

    # remove the workspace (including all files and directories)
    # the part of the tree as resolved by getPath() is pruned recursively
    # if preserve_top is true then the directory specified by getPath() will
    # be preserved
    def remove(self, preserve_top=False):
        try:
            import shutil
            logger.debug('removing %s', self.getPath())
            if os.path.exists(self.getPath()):

                try:
                    import time
                    remove_path = os.path.dirname(self.getPath()) + "_" + str(time.time()) + '__to_be_deleted_'
                    logger.debug("Moving Path: %s to: %s ahead of delete operation" % (self.getPath(), remove_path))
                    os.rename(self.getPath(), remove_path)
                    logger.debug("Move completed")
                except OSError as err:
                    logger.debug("Move Error!")
                    logger.debug("Error moving file for deletion, not using new path")
                    logger.debug("Error: %s" % err)
                    remove_path = self.getPath()

                self.__removeTrials = 0

                def retryRemove(function, path, excinfo):
                    """ Address AFS/NSF problems with left-over lock files which prevents
                    the 'shutil.rmtree' to delete the directory (the idea is to wait a bit 
                    for the fs to automatically remove these lock files and try again)
                    """
                    self.__removeTrials += 1
                    if self.__removeTrials <= 5:
                        logger.debug('Cannot delete %s (retry count=%s) ... Will wait a bit and try again' % (self.getPath(), self.__removeTrials))
                        time.sleep(0.5)
                        shutil.rmtree(remove_path, ignore_errors=False, onerror=retryRemove)
                    else:
                        exctype, value = excinfo[:2]
                        logger.warning('Cannot delete %s after %s retries due to:  %s:%s (there might some AFS/NSF lock files left over)' %
                                                                                        (self.getPath(), self.__removeTrials, exctype, value))

                shutil.rmtree(remove_path, ignore_errors=False, onerror=retryRemove)
                logger.debug('removed %s', remove_path)
                if preserve_top and not os.path.exists(self.getPath()):
                    logger.debug('preserving the topdir: mkdir %s' % self.getPath())
                    os.mkdir(self.getPath())
            else:
                logger.debug('%s : DOES NOT EXIST', self.getPath())

        # FIXME: error strategy
        except OSError as x:
            logger.debug("OSError: %s" % x)
            raise
        except Exception as x:
            logger.debug("Exception: %s" % x)
            raise


def gettop():
    from GangaCore.Utility.Config import getConfig
    c = getConfig('Configuration')
    return os.path.join(c['gangadir'], 'workspace', c['user'], c['repositorytype'])


class InputWorkspace(FileWorkspace):

    """ Part of the workspace for storing input sandbox.
    """

    def __init__(self):
        workspace_top = gettop()
        super(InputWorkspace, self).__init__(workspace_top, subpath='input')


class OutputWorkspace(FileWorkspace):

    """ Part of the workspace for storing output sandbox.
    """

    def __init__(self):
        workspace_top = gettop()
        super(OutputWorkspace, self).__init__(workspace_top, subpath='output')


class DebugWorkspace(FileWorkspace):

    """ Part of the workspace for storing output sandbox.
    """

    def __init__(self):
        workspace_top = gettop()
        super(DebugWorkspace, self).__init__(workspace_top, subpath='debug')


#
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2008/07/17 16:40:49  moscicki
# migration of 5.0.2 to HEAD
#
# the doc and release/tools have been taken from HEAD
#
# Revision 1.16.28.7  2008/04/18 08:17:24  moscicki
# minor fix
#
# Revision 1.16.28.6  2008/04/02 15:25:24  moscicki
# bugfix: #31691 j.remove() removes workspace for all jobs (not just job in question)
#
# Revision 1.16.28.5  2008/03/11 15:20:30  moscicki
# merge from Ganga-5-0-restructure-config-branch
#
# Revision 1.16.28.4.2.1  2008/03/07 13:36:07  moscicki
# removal of [DefaultJobRepository] and [FileWorkspace]
# new options in [Configuration] user, gangadir, repositorytype, workspacetype
#
# Revision 1.16.28.4  2008/02/05 12:29:56  amuraru
# bugfix #32850
#
# Revision 1.16.28.3  2007/12/10 19:25:03  amuraru
# merged changes from Ganga 4.4.4
#
# Revision 1.16.28.2  2007/10/25 11:39:33  roma
# Config update
#
# Revision 1.16.28.1  2007/10/12 13:56:23  moscicki
# merged with the new configuration subsystem
#
# Revision 1.16.30.1  2007/09/25 09:45:11  moscicki
# merged from old config branch
#
# Revision 1.16.8.1  2007/06/18 07:44:51  moscicki
# config prototype
#
# Revision 1.17  2007/11/23 15:11:03  amuraru
# fixed bug #22428 and #29825
#
# Revision 1.19  2008/04/02 15:11:33  moscicki
# bugfix: #31691 j.remove() removes workspace for all jobs (not just job in question)
#
# Revision 1.18  2008/01/25 09:42:20  amuraru
# fixed bug #32850
#
# Revision 1.17  2007/11/23 15:11:03  amuraru
# fixed bug #22428 and #29825
#
# Revision 1.16  2006/07/27 20:02:01  moscicki
# comments
#
# Revision 1.15  2006/02/10 14:07:03  moscicki
# code cleanup
#
# Revision 1.14  2005/11/14 10:03:44  moscicki
# possibility to leave or remove the empty workspace directory
#
# Revision 1.13  2005/10/12 13:35:23  moscicki
# renamed _gangadir into gangadir
#
# Revision 1.12  2005/10/07 15:06:57  moscicki
# renamed __Ganga4__ into _gangadir
#
# Revision 1.11  2005/09/23 09:06:41  moscicki
# splittree option now defaults to 0 and is obsolete
#
# Revision 1.10  2005/09/06 12:02:14  moscicki
# minor fix
#
# Revision 1.9  2005/08/23 17:18:13  moscicki
# using new File methods, removed Karl's fix as obsolete
#
# Revision 1.8  2005/08/16 10:59:27  karl
# KH: Allow old and new style configuration
#
# Revision 1.7  2005/08/10 15:01:10  moscicki
# Fixed the exception handling for makedirs -- do not mask runtime errors, ignore EEXIST
#
# Revision 1.6  2005/08/10 09:45:36  andrew
# Added a subdir to File and FileBuffer objects. Changed the writefile method
# in FileWorspace to use the subdirectory
#
#
#
