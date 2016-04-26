from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LocalFile.py,v 0.1 2011-09-29 15:40:00 idzhunov Exp $
##########################################################################

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem

from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile

from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File import FileBuffer

import Ganga.Utility.logging

import errno
import re
import os
import os.path
import copy

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
    _exportmethods = ["location", "remove", "accessURL", "processOutputWildcardMatches", "hasMatchedFiles"]

    def __init__(self, namePattern='', localDir='', **kwds):
        """ name is the name of the output file that is going to be processed
            in some way defined by the derived class
        """
        super(LocalFile, self).__init__()

        self.tmp_pwd = None
        self.output_location = None

        if isinstance(namePattern, str):
            self.namePattern = namePattern
        elif isinstance(namePattern, File):
            self.namePattern = os.path.basename(namePattern.name)
            self.localDir = os.path.dirname(namePattern.name)
        elif isinstance(namePattern, FileBuffer):
            namePattern.create()
            self.namePattern = os.path.basename(namePattern.name)
            self.localDir = os.path.dirname(namePattern.name)
        else:
            logger.error("Unkown type: %s . Cannot Create LocalFile from this!" % type(namePattern))

        if isinstance(localDir, str):
            if localDir != '':
                self.localDir = localDir
            else:
                this_pwd = os.path.abspath('.')
                self.tmp_pwd = this_pwd
        else:
            logger.error("Unkown type: %s . Cannot set LocalFile localDir using this!" % type(localDir))

    def __construct__(self, args):

        self.tmp_pwd = None
        self.output_location = None

        self.localDir = ''

        from Ganga.GPIDev.Lib.File.SandboxFile import SandboxFile
        if len(args) == 1 and isinstance(args[0], str):
            self.namePattern = args[0]
        elif len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], str):
            self.namePattern = args[0]
            self.localDir = args[1]
        elif len(args) == 1 and isinstance(args[0], SandboxFile):
            super(LocalFile, self).__construct__(args)

        if self.localDir == '' and self.namePattern != '':
            this_pwd = os.path.abspath('.')
            self.tmp_pwd = this_pwd

    def __repr__(self):
        """Get the representation of the file."""

        return "LocalFile(namePattern='%s')" % self.namePattern

    def location(self):
        return self.getFilenameList()

    def accessURL(self):
        URLs = []
        for file in self.location():
            URLs.append('file://' + os.path.join(os.sep, file))
        return URLs

    def processOutputWildcardMatches(self):
        """This collects the subfiles for wildcarded output LocalFile"""
        import glob

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern

        sourceDir = self.getJobObject().outputdir
        if regex.search(fileName) is not None:

            for currentFile in glob.glob(os.path.join(sourceDir, fileName)):

                d = LocalFile(namePattern=os.path.basename(currentFile))
                d.compressed = self.compressed

                self.subfiles.append(GPIProxyObjectFactory(d))

    def processWildcardMatches(self):

        if self.subfiles:
            return self.subfiles

        import glob

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern

        sourceDir = self.localDir

        if regex.search(fileName) is not None:
            for currentFile in glob.glob(os.path.join(sourceDir, fileName)):
                d = LocalFile(namePattern=os.path.basename(
                    currentFile), localDir=os.path.dirname(currentFile))
                d.compressed = self.compressed

                self.subfiles.append(GPIProxyObjectFactory(d))

    def getFilenameList(self):
        """Return the files referenced by this LocalFile"""
        filelist = []
        self.processWildcardMatches()
        if self.subfiles:
            for f in self.subfiles:
                filelist.append(os.path.join(f.localDir, f.namePattern))
        else:
            if self.localDir == '':
                if os.path.exists(os.path.join(self.tmp_pwd, self.namePattern)):
                    self.localDir = self.tmp_pwd
                    logger.debug("File: %s found, Setting localDir: %s" % (self.namePattern, self.localDir))
                else:
                    this_pwd = os.path.abspath('.')
                    now_tmp_pwd = this_pwd
                    if os.path.exists(os.path.join(now_tmp_pwd, self.namePattern)):
                        self.localDir = now_tmp_pwd
                        logger.debug("File: %s found, Setting localDir: %s" % (self.namePattern, self.localDir))
                    else:
                        logger.debug("File: %s NOT found, NOT setting localDir: %s !!!" % (self.namePattern, self.localDir))

            filelist.append(os.path.join(self.localDir, self.namePattern))

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
        if job:
            fname = self.namePattern
            if self.compressed:
                fname += ".gz"

            if os.path.isfile(os.path.join(job.getOutputWorkspace().getPath(), fname)):
                return True

        return False

    def remove(self):

        for this_file in self.getFilenameList():
            _actual_delete = False
            keyin = None
            while keyin is None:
                keyin = raw_input("Do you want to remove the LocalFile: %s ? ([y]/n) " % this_file)
                if keyin in ['y', '']:
                    _actual_delete = True
                elif keyin == 'n':
                    _actual_delete = False
                else:
                    logger.warning("y/n please!")
                    keyin = None
            if _actual_delete:
                if not os.path.exists(this_file):
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


## rcurrie Attempted to implement for 6.1.9 but commenting out due to not being able to correctly make use of setLocation

#    def getWNScriptDownloadCommand(self, indent):
#
#        script = """
####INDENT###os.system('###CP_COMMAND')
#
#"""
#        full_path = os.path.join(self.localDir, self.namePattern)
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
#    def setLocation(self):
#
#        job = self.getJobObject()
#
#        self.output_location = job.getOutputWorkspace(create=True).getPath()
#
#        return

