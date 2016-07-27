from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LocalFile.py,v 0.1 2011-09-29 15:40:00 idzhunov Exp $
##########################################################################

import errno
import re
import os
from os import path
import copy
import shutil

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem

from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile

from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File import FileBuffer
from Ganga.Utility.files import expandfilename

import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

regex = re.compile('[*?\[\]]')

class LocalFile(IGangaFile):

    """LocalFile represents base class for output files, such as MassStorageFile, LCGSEFile, etc 
    """
    _schema = Schema(Version(1, 1), {'namePattern': SimpleItem(defvalue="", doc='pattern of the file name'),
                                     'localDir': SimpleItem(defvalue="", doc='local dir where the file is stored, used from get and put methods'),
                                     'subfiles': ComponentItem(category='gangafiles', defvalue=[], hidden=1,
                                                sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),
                                     'compressed': SimpleItem(defvalue=False, typelist=[bool], protected=0, doc='wheather the output file should be compressed before sending somewhere'),
                                     #'output_location': SimpleItem(defvalue=None, typelist=[str, None], hidden=1, copyable=1, doc="path of output location on disk")
                                     })
    _category = 'gangafiles'
    _name = "LocalFile"
    _exportmethods = ["location", "remove", "accessURL"]

    def __init__(self, namePattern='', localDir='', **kwds):
        """ name is the name of the output file that is going to be processed
            in some way defined by the derived class
        """
        super(LocalFile, self).__init__()

        self.tmp_pwd = None
        self.output_location = None

        if isinstance(namePattern, str):
            self.namePattern = namePattern
            if localDir:
            	self.localDir = localDir
        elif isinstance(namePattern, File):
            self.namePattern = path.basename(namePattern.name)
            self.localDir = path.dirname(namePattern.name)
        elif isinstance(namePattern, FileBuffer):
            namePattern.create()
            self.namePattern = path.basename(namePattern.name)
            self.localDir = path.dirname(namePattern.name)
        else:
            logger.error("Unkown type: %s . Cannot Create LocalFile from this!" % type(namePattern))


    def __setattr__(self, attr, value):
        """
        This is an overloaded setter method to make sure that we're auto-expanding the filenames of files which exist.
        In the case we're assigning any other attributes the value is simply passed through
        Args:
            attr (str): This is the name of the attribute which we're assigning
            value (unknown): This is the value being assigned.
        """
        actual_value = value
        if attr == 'namePattern':
            if len(value.split(os.sep)) > 1:
                this_dir = path.dirname(value)
                super(LocalFile, self).__setattr__('localDir', this_dir)
            actual_value = path.basename(value)
        elif attr == 'localDir':
            if value:
                new_value = path.abspath(expandfilename(value))
                if path.exists(new_value):
                    actual_value = new_value

        super(LocalFile, self).__setattr__(attr, actual_value)
        

    def __repr__(self):
        """Get the representation of the file."""
        return "LocalFile(namePattern='%s', localDir='%s')" % (self.namePattern, self.localDir)

    def location(self):
        return self.getFilenameList()

    def accessURL(self):
        URLs = []
        for file in self.location():
            URLs.append('file://' + path.join(os.sep, file))
        return URLs

    def setLocation(self):
        """This collects the subfiles for wildcarded output LocalFile"""
        import glob

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern

        sourceDir = self.getJobObject().outputdir
        
        if self.localDir:
            fileName = path.join(self.localDir, fileName)

        for currentFile in glob.glob(path.join(sourceDir, fileName)):

            base_name = path.basename(currentFile)

            d = LocalFile(base_name)
            d.compressed = self.compressed

            self.subfiles.append(d)

    def processWildcardMatches(self):

        if self.subfiles:
            return self.subfiles

        import glob

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern

        sourceDir = self.localDir

        if regex.search(fileName) is not None:
            for currentFile in glob.glob(path.join(sourceDir, fileName)):
                d = LocalFile(namePattern=path.basename(
                    currentFile), localDir=path.dirname(currentFile))
                d.compressed = self.compressed

                self.subfiles.append(d)

    def getFilenameList(self):
        """Return the files referenced by this LocalFile"""
        filelist = []
        self.processWildcardMatches()
        if self.subfiles:
            for f in self.subfiles:
                filelist.append(path.join(f.localDir, f.namePattern))
        else:
            if path.exists(path.join(self.localDir, self.namePattern)):
                logger.debug("File: %s found, Setting localDir: %s" % (self.namePattern, self.localDir))

            filelist.append(path.join(self.localDir, self.namePattern))

        return filelist

    def hasMatchedFiles(self):
        """
        OK for checking subfiles but of no wildcards, need to actually check file exists
        """

        # check for subfiles
        if len(self.subfiles) > 0:
            # we have subfiles so we must have actual files associated
            return True
        else:
            if self.containsWildcards():
                return False

        # check if single file exists (no locations field to try)
        job = self.getJobObject()
        fname = self.namePattern
        if self.compressed:
            fname += ".gz"

        if path.isfile(path.join(job.getOutputWorkspace().getPath(), fname)):
            return True

        return False

    def remove(self):

        for this_file in self.getFilenameList():
            _actual_delete = False
            keyin = None
            while keyin is None:
                keyin = raw_input("Do you want to remove the LocalFile: %s ? ([y]/n) " % this_file)
                if keyin.lower() in ['y', '']:
                    _actual_delete = True
                elif keyin.lower() == 'n':
                    _actual_delete = False
                else:
                    logger.warning("y/n please!")
                    keyin = None
            if _actual_delete:
                if not path.exists(this_file):
                    logger.warning(
                        "File %s did not exist, can't delete" % this_file)
                else:
                    logger.info("Deleting: %s" % this_file)

                    import time
                    remove_filename = this_file + "_" + str(time.time()) + '__to_be_deleted_'
                    try:
                        os.rename(this_file, remove_filename)
                    except Exception as err:
                        logger.warning("Error in first stage of removing file: %s" % this_file)
                        remove_filename = this_file

                    try:
                        os.remove(remove_filename)
                    except OSError as err:
                        if err.errno != errno.ENOENT:
                            logger.error("Error in removing file: %s" % remove_filename)
                            raise
                        pass

        return

    def put(self):
	"""
        Copy the file to the detination (in the case of LocalFile the localDir)
        """
        #FIXME this method should be written to work with some other parameter than localDir for job outputs but for now this 'works'
        if self.localDir:
            try:
                job = self.getJobObject()
            except AssertionError as err:
                return

            # Copy to 'desitnation'

            if path.isfile(path.join(job.outputdir, self.namePattern)):
                if not path.exists(path.join(job.outputdir, self.localDir)):
                    os.makedirs(path.join(job.outputdir, self.localDir))
                shutil.copy(path.join(job.outputdir, self.namePattern),
                            path.join(job.outputdir, self.localDir, self.namePattern))
           

    def cleanUpClient(self):
        """
        This performs the cleanup method on the client output workspace to remove temporary files
        """
        # For LocalFile this is where the file is stored so don't remove it
        pass

## rcurrie Attempted to implement for 6.1.9 but commenting out due to not being able to correctly make use of setLocation

#    def getWNScriptDownloadCommand(self, indent):
#
#        script = """
####INDENT###os.system('###CP_COMMAND')
#
#"""
#        full_path = path.join(self.localDir, self.namePattern)
#        replace_dict = {'###INDENT###' : indent, '###CP_COMMAND###' : 'cp %s .' % full_path}
#
#        for k, v in replace_dict.iteritems():
#            script = script.replace(k, v)
#
#        return script
#
#
#    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
#
#        cp_template = """
####INDENT###os.system("###CP_COMMAND###")
#"""
#        script = ""
#
#        for this_file in outputFiles:
#            filename = this_file.namePattern
#            cp_cmd = 'cp %s %s' % (filename, self.output_location)
#
#            this_cp = str(cp_template)
#
#            replace_dict = {'###INDENT###' : indent, '###CP_COMMAND###' : cp_cmd}
#
#            for k, v in replace_dict.iteritems():
#                print("Replace %s : %s" % (k, v))
#                this_cp = this_cp.replace(k, v)
#
#            script = this_cp
#            break
#
#        return script
#

