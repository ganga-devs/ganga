##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: FileBuffer.py,v 1.1 2008-07-17 16:40:53 moscicki Exp $
##########################################################################

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

import os


class FileBuffer(object):

    """ FileBuffer represents a file in memory which has not been yet created.
        This is a handy way of creating small wrapper scripts to be generated on the fly.
    """

    # Added a subdir (see File.py for comments) - AM
    def __init__(self, name, contents, subdir=os.curdir, executable=0):
        """ name is the name of the file to be created
            contents is the text with file contents or a file-object which will be read()
            executable indicates if a file is create()'ed with executable permissions
        """
        self.name = name
        self._contents = contents
        self.subdir = subdir
        self.executable = executable

    def getPathInSandbox(self):
        """return a relative location of a file in a sandbox: subdir/name"""
        from Ganga.Utility.files import real_basename
        return os.path.join(self.subdir, real_basename(self.name))

    def getContents(self):
        """return a string with the contents of the file buffer"""
        logger.debug("Reading FileBuffer: %s" % self.name)
        if isinstance(self._contents, str):
            return self._contents
        else:
            return self._contents.read()

    def create(self, outname=None):
        """create a file in a local filesystem as 'outname' """
        filename = self.name
        if outname is not None:
            filename = outname

        with open(filename, 'w') as thisfile:
            thisfile.write(self.getContents())

        if self.executable:
            from Ganga.Utility.files import chmod_executable
            chmod_executable(filename)

        logger.debug("Created %s in: %s" %
                     (filename, os.path.realpath(filename)))

        return self

    def isExecutable(self):
        """ return true if a file is create()'ed with executable permissions"""
        return self.executable


#
#
#
# $Log: not supported by cvs2svn $
# Revision 1.5  2007/08/28 08:26:35  moscicki
# fixed typo
#
# Revision 1.4  2005/08/23 17:07:23  moscicki
# Added executable flag for FileBuffer.
# Added create method for File and FileBuffer.
# Added getPathInSandbox() method.
#
# Revision 1.3  2005/08/10 09:45:36  andrew
# Added a subdir to File and FileBuffer objects. Changed the writefile method
# in FileWorspace to use the subdirectory
#
#
#
#
