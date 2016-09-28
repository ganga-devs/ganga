from __future__ import absolute_import
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: MassStorageFile.py,v 0.1 2011-11-09 15:40:00 idzhunov Exp $
##########################################################################
import errno
import re
import os
import os.path
import copy
import glob
import inspect
import time
from fnmatch import fnmatch
from pipes import quote

from Ganga.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem

from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.Utility import Shell
from Ganga.Utility.logging import getLogger
from Ganga.GPIDev.Adapters.IGangaFile import IGangaFile
from Ganga.GPIDev.Base.Objects import _getName
from Ganga.Utility.files import expandfilename

import Ganga.Utility.Config

logger = getLogger()

class MassStorageFile(IGangaFile):
    """MassStorageFile represents a class marking a file to be written into mass storage (like Castor at CERN)
    """
    _schema = Schema(Version(1, 1), {'namePattern': SimpleItem(defvalue="", doc='pattern of the file name'),
                                     'localDir': SimpleItem(defvalue="", copyable=1, doc='local dir where the file is stored, used from get and put methods'),
                                     'joboutputdir': SimpleItem(defvalue="", doc='outputdir of the job with which the outputsandbox file object is associated'),
                                     'locations': SimpleItem(defvalue=[], copyable=1, typelist=[str], sequence=1, doc="list of locations where the outputfiles are uploaded"),
                                     'outputfilenameformat': SimpleItem(defvalue=None, typelist=[str, None], protected=0,\
                                                    doc="keyword path to where the output should be uploaded, i.e. /some/path/here/{jid}/{sjid}/{fname},\
                                                        if this field is not set, the output will go in {jid}/{sjid}/{fname} or in {jid}/{fname}\
                                                        depending on whether the job is split or not"),
                                     'inputremotedirectory': SimpleItem(defvalue=None, typelist=[str, None], protected=0, doc="Directory on mass storage where the file is stored"),
                                     'subfiles': ComponentItem(category='gangafiles', defvalue=[], hidden=1, sequence=1, copyable=0,\
                                                    doc="collected files from the wildcard namePattern"),
                                     'failureReason': SimpleItem(defvalue="", protected=1, copyable=0, doc='reason for the upload failure'),
                                     'compressed': SimpleItem(defvalue=False, typelist=[bool], protected=0, doc='wheather the output file should be compressed before sending somewhere')
                                     })

    _category = 'gangafiles'
    _name = "MassStorageFile"
    _exportmethods = ["location", "get", "put", "setLocation", "remove", "accessURL", "copyTo"]

    def __init__(self, namePattern='', localDir='', **kwds):
        """
        MassStorageFile construction
        Args:
            namePattern (str): is the pattern of the output file that has to be written into mass storage
            localDir (str): This is the optional local directory of a file to be uploaded to mass storage
        """
        super(MassStorageFile, self).__init__()
        self._setNamePath(_namePattern=namePattern, _localDir=localDir)
        self.locations = []

        self.shell = Shell.Shell()

    def __setattr__(self, attr, value):
        """
        This is an overloaded setter method to make sure that we're auto-expanding the filenames of files which exist.
        In the case we're assigning any other attributes the value is simply passed through
        Args:
            attr (str): This is the name of the attribute which we're assigning
            value (unknown): This is the value being assigned.
        """
        actual_value = value
        if attr == "namePattern":
            actual_value = os.path.basename(value)
            this_localDir = os.path.dirname(value)
            if this_localDir:
                self.localDir = this_localDir
        if attr == "localDir":
            if value:
                actual_value = os.path.abspath(expandfilename(value))

        super(MassStorageFile, self).__setattr__(attr, actual_value)

    def _setNamePath(self, _namePattern='', _localDir=''):
        if _namePattern != '' and _localDir == '':
            self.namePattern = os.path.basename(_namePattern)
            if not os.path.dirname(_namePattern):
                if os.path.isfile(os.path.join(os.getcwd(), os.path.basename(_namePattern))):
                    self.localDir = os.getcwd()
            else:
                self.localDir = os.path.dirname(_namePattern)
        elif _namePattern != '' and _localDir != '':
            self.namePattern = _namePattern
            self.localDir = _localDir


    def _on_attribute__set__(self, obj_type, attrib_name):
        # This is defining the object as uncopyable from outputfiles... do we want this mechanism still?
        r = copy.deepcopy(self)
        from Ganga.GPIDev.Lib.Job.Job import Job
        if isintance(obj_type, Job) and attrib_name == 'outputfiles':
            r.locations = []
            r.localDir = ''
            r.failureReason = ''
        return r

    def __repr__(self):
        """Get the representation of the file."""

        return "%s(namePattern='%s')" % (_getName(self), self.namePattern)

    def mass_line_processor(self, line):
        """ This function splits the input line from the post-processsing system to define where this file is:
        Args:
            line(str): This is expected to be in the format of the postprocessor file from jobs transfering files on the WN
        """
        lineParts = line.split()
        pattern = lineParts[1]
        outputPath = lineParts[2]
        split_name = os.path.splitext(outputPath)
        if split_name[1] == '.gz':
            name = split_name[0]
        else:
            name = outputPath

        if self.containsWildcards():
            if outputPath == 'ERROR':
                logger.error("Failed to upload file to mass storage")
                logger.error(line[line.find('ERROR') + 5:])
                d = copy.deepcopy(self)
                d.namePattern = pattern
                d.compressed = self.compressed
                d.failureReason = line[line.find('ERROR') + 5:]
                self.subfiles.append(d)
            else:
                if pattern == self.namePattern:
                    d = copy.deepcopy(self)
                    d.namePattern = name
                    self.subfiles.append(d)
                    d.mass_line_processor(line)
        elif name == self.namePattern:
            if outputPath == 'ERROR':
                logger.error("Failed to upload file to mass storage")
                logger.error(line[line.find('ERROR') + 5:])
                self.failureReason = line[line.find('ERROR') + 5:]
                return
        self.locations = [outputPath.strip('\n')]

    def setLocation(self):
        """
        Sets the location of output files that were uploaded to mass storage from the WN
        """
        job = self.getJobObject()

        postprocessLocationsPath = os.path.join(job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])
        if not os.path.exists(postprocessLocationsPath):
            return

        for line in open(postprocessLocationsPath, 'r'):

            if line.strip() == '':
                continue

            if line.startswith('massstorage'):
                self.mass_line_processor(line.strip())

    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        tmpLocations = []
        if self.subfiles:
            for i in self.subfiles:
                tmpLocations.append(i.locations)
        else:
            tmpLocations = self.locations
        return tmpLocations

    def internalCopyTo(self, targetPath):
        """
        Copy a the file to the local storage using the get mechanism
        Args:
            targetPath (str): Target path where the file is to copied to
        """
        to_location = targetPath

        cp_cmd = getConfig('Output')[_getName(self)]['uploadOptions']['cp_cmd']

        for location in self.locations:
            targetLocation = os.path.join(to_location, os.path.basename(location))
            MassStorageFile.execSyscmdSubprocess('%s %s %s' % (cp_cmd, quote(location), quote(targetLocation)))

    def getWNScriptDownloadCommand(self, indent):
        ## FIXME fix me for the situation of multiple files?

        script = """\n

###INDENT###os.system(\'###CP_COMMAND###\')

"""
        cp_cmd = '%s %s .' % (getConfig('Output')[_getName(self)]['uploadOptions']['cp_cmd'], quote(self.locations[0]))

        replace_dict = { '###INDENT###' : indent, '###CP_COMMAND###' : cp_cmd }

        for k, v in replace_dict.iteritems():
            script = script.replace(str(k), str(v))

        return script

    def checkDirExists(self, checkPath):
        """
        Checks to see if a directory exists or not within the remote storage
        Args:
            checkPath (str): This is the path we're checking to see if exists
        """

        massStorageConfig = getConfig('Output')[_getName(self)]['uploadOptions']
        ls_cmd = massStorageConfig['ls_cmd']
        massStoragePath = massStorageConfig['path']

        massStoragePath = os.path.join(massStoragePath, checkPath)

        pathToDirName = os.path.dirname(massStoragePath)

        (exitcode, mystdout, mystderr) = MassStorageFile.execSyscmdSubprocess('%s %s' % (ls_cmd, quote(pathToDirName)))
        if exitcode != 0:
            #logger.error("Error running ls_cmd (%s) on %s" % (ls_cmd, quote(pathToDirName)))
            #self.handleUploadFailure(mystderr, '1) %s %s' % (ls_cmd, pathToDirName))
            return False

        for directory in mystdout.split('\n'):
            if directory.strip() == os.path.basename(massStoragePath):
                return True
        
        return False

    def getOutputFilename(self):
        """
        """

        filePath, fileName = super(MassStorageFile, self).getOutputFilename()

        massStorageConfig = getConfig('Output')[_getName(self)]['uploadOptions']
        filePath = os.path.join(massStorageConfig['path'], filePath)

        return filePath, fileName

    def makeStorageDir(self, wantedPath):
        """
        Creates a folder on the mass Storage corresponding to the given path
        Args:
            massStoragePath (str): This is the path we want to make if it doesn't exist.
        """

        massStorageConfig = getConfig('Output')[_getName(self)]['uploadOptions']

        mkdir_cmd = massStorageConfig['mkdir_cmd']
        massStoragePath = massStorageConfig['path']

        if not wantedPath.startswith(massStoragePath):
            wantedPath = os.path.join(massStoragePath, wantedPath)

        directoryExists = self.checkDirExists(wantedPath)

        if not directoryExists:
            (exitcode, mystdout, mystderr) = MassStorageFile.execSyscmdSubprocess('%s -p %s' % (mkdir_cmd, quote(wantedPath)))
            if exitcode != 0:
                self.handleUploadFailure(mystderr, '2) %s %s' % (mkdir_cmd, wantedPath))
                raise GangaException(mystderr)

    def uploadTo(self, targetPath):
        """
        """

        sourcePath = os.path.join(self.localDir, self.namePattern)

        massStorageConfig = getConfig('Output')[_getName(self)]['uploadOptions']
        cp_cmd = massStorageConfig['cp_cmd']

        if not self.checkDirExists(''):
            massStoragePath = massStorageConfig['path']
            logger.error("Can't upload as %s cannot be found!" % massStoragePath)
            return False

        self.makeStorageDir(os.path.dirname(targetPath))

        (exitcode, mystdout, mystderr) = MassStorageFile.execSyscmdSubprocess('%s %s %s' % (cp_cmd, quote(sourcePath), quote(targetPath)))
        if exitcode != 0:
            self.handleUploadFailure(mystderr, '5) %s %s %s' % (cp_cmd, quote(sourcePath), quote(targetPath)))
            return False
        else:
            logger.info('%s successfully uploaded to mass storage as %s' % (quote(sourcePath), quote(targetPath)))
            if targetPath not in self.locations:
                self.locations.append(targetPath)
            return True

    def validate(self):

        # if the user has set outputfilenameformat, validate for presence of
        # jid, sjid and fname keywords depending on job type - split or
        # non-split
        if self.outputfilenameformat != None:

            searchFor = ['{fname}']
            isJob = False
            isSplitJob = False

            if self._getParent() != None:

                isJob = True

                if stripProxy(self.getJobObject()).master is not None:

                    isSplitJob = True
                    searchFor.append('{sjid}')

            missingKeywords = []

            for item in searchFor:
                if self.outputfilenameformat.find(item) == -1:
                    missingKeywords.append(item)

            if len(missingKeywords):
                return (False, 'Error in %s.outputfilenameformat field : missing keywords %s ' % (_getName(self), ','.join(missingKeywords)))

            if isSplitJob == False and self.outputfilenameformat.find('{sjid}') > -1:
                return (False, 'Error in %s.outputfilenameformat field :  job is non-split, but {\'sjid\'} keyword found' % _getName(self))

            if isJob == False and self.outputfilenameformat.find('{sjid}') > -1:
                return (False, 'Error in %s.outputfilenameformat field :  no parent job, but {\'sjid\'} keyword found' % _getName(self))

            if isJob == False and self.outputfilenameformat.find('{jid}') > -1:
                return (False, 'Error in %s.outputfilenameformat field :  no parent job, but {\'jid\'} keyword found' % _getName(self))

            invalidUnixChars = ['"', ' ']
            test = self.outputfilenameformat.replace('{jid}', 'a').replace('{sjid}', 'b').replace('{fname}', 'c')

            for invalidUnixChar in invalidUnixChars:
                if test.find(invalidUnixChar) > -1:
                    return (False, 'Error in %s.outputfilenameformat field :  invalid char %s found' % (_getName(self), invalidUnixChar))

        return (True, '')

    def handleUploadFailure(self, error, cmd_run_str=''):
        """
        Function to display what went wrong with an associated Job id if there is one and to assign failureReason for future.
        Args:
            error (str): This is the error which was given from the shell command
            cmd_run_str (str): This is a string related to but not always exactly the command run.
        """

        self.failureReason = error
        if self._getParent() != None:
            logger.error("Job %s failed. One of the job.outputfiles couldn't be uploaded because of %s" % (str(self._getParent().fqid), self.failureReason))
        else:
            logger.error("The file can't be uploaded because of %s" % (self.failureReason))
        if cmd_run_str:
            logger.error("Attempted to run: '%s'" % (cmd_run_str))

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
        massStorageCommands = []

        massStorageConfig = getConfig('Output')[_getName(self)]['uploadOptions']

        for outputFile in outputFiles:

            outputfilenameformat = 'None'
            if outputFile.outputfilenameformat != None and outputFile.outputfilenameformat != '':
                outputfilenameformat = outputFile.outputfilenameformat

            massStorageCommands.append(['massstorage', outputFile.namePattern, outputfilenameformat,
                                        massStorageConfig['mkdir_cmd'],  massStorageConfig['cp_cmd'],
                                        massStorageConfig['ls_cmd'], massStorageConfig['path']])

        script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                        'scripts/MassStorageFileWNScript.py.template')

        from Ganga.GPIDev.Lib.File import FileUtils
        script = FileUtils.loadScript(script_location, '###INDENT###')

        jobfqid = self.getJobObject().fqid

        jobid = jobfqid
        subjobid = ''

        if (jobfqid.find('.') > -1):
            jobid = jobfqid.split('.')[0]
            subjobid = jobfqid.split('.')[1]

        replace_dict = {'###MASSSTORAGECOMMANDS###' : repr(massStorageCommands),
                        '###PATTERNSTOZIP###' : str(patternsToZip),
                        '###INDENT###' : indent,
                        '###POSTPROCESSLOCATIONSFP###' : postProcessLocationsFP,
                        '###FULLJOBDIR###' : str(jobfqid.replace('.', os.path.sep)),
                        '###JOBDIR###' : str(jobid),
                        '###SUBJOBDIR###' : str(subjobid)}

        for k, v in replace_dict.iteritems():
            script = script.replace(str(k), str(v))

        return script

    def processWildcardMatches(self):
        if self.subfiles:
            return self.subfiles

        if self.containsWildcards():
            ls_cmd = getConfig('Output')[_getName(self)]['uploadOptions']['ls_cmd']
            exitcode, output, m = self.shell.cmd1(ls_cmd + ' ' + self.inputremotedirectory, capture_stderr=True)

            for filename in output.split('\n'):
                if fnmatch(filename, self.namePattern):
                    subfile = copy.deepcopy(self)
                    subfile.namepattern = filename
                    subfile.inputremotedirectory = self.inputremotedirectory

                    self.subfiles.append(subfile)

    def remove(self, force=False, removeLocal=False):
        """
        Removes file from remote storage ONLY by default
        """
        massStorageConfig = getConfig('Output')[_getName(self)]['uploadOptions']
        rm_cmd = massStorageConfig['rm_cmd']

        if force == True:
            _auto_delete = True
        else:
            _auto_delete = False

        for i in self.locations:

            if not _auto_delete:

                keyin = None

                while keyin is None:
                    keyin = raw_input("Do you want to delete file %s at Location: %s ? [y/n] " % (str(self.namePattern), str(i)))
                    if keyin.lower() == 'y':
                        _delete_this = True
                    elif keyin.lower() == 'n':
                        _delete_this = False
                    else:
                        logger.warning("y/n please!")
                        keyin = None
            else:
                _delete_this = True

            if _delete_this:
                logger.info("Deleting File at Location: %s")
                MassStorageFile.execSyscmdSubprocess('%s %s' % (rm_cmd, quote(i)))
                self.locations.pop(i)

        if removeLocal:

            sourceDir = ''
            if self.localDir == '':
                _CWD = os.getcwd()
                if os.path.isfile(os.path.join(_CWD, self.namePattern)):
                    sourceDir = _CWD
            else:
                sourceDir = self.localDir

            _localFile = os.path.join(sourceDir, self.namePattern)

            if os.path.isfile(_localFile):

                if force:
                    _actual_delete = True
                else:

                    keyin = None
                    while keyin is None:
                        keyin = raw_input("Do you want to remove the local File: %s ? ([y]/n) " % str(_localFile))
                        if keyin.lower() in ['y', '']:
                            _actual_delete = True
                        elif keyin.lower() == 'n':
                            _actual_delete = False
                        else:
                            logger.warning("y/n please!")
                            keyin = None

                if _actual_delete:
                    remove_filename = _localFile + "_" + str(time.time()) + '__to_be_deleted_'

                    try:
                        os.rename(_localFile, remove_filename)
                    except OSError as err:
                        logger.warning("Error in first stage of removing file: %s" % remove_filename)
                        remove_filename = _localFile

                    try:
                        os.remove(remove_filename)
                    except OSError as err:
                        if err.errno != errno.ENOENT:
                            logger.error("Error in removing file: %s" % str(remove_filename))
                            raise
                        pass
        return

    def accessURL(self):

        # Need to come up with a prescription based upon the server address and
        # file on EOS or elsewhere to return a full URL which we can pass to
        # ROOT...

        protoPath = getConfig('Output')[_getName(self)]['defaultProtocol']

        myLocations = self.location()

        accessURLs = []

        for file in myLocations:
            accessURLs.append(protoPath + os.path.join(os.sep, file))

        return accessURLs

# add MassStorageFile objects to the configuration scope (i.e. it will be
# possible to write instatiate MassStorageFile() objects via config file)
Ganga.Utility.Config.config_scope['MassStorageFile'] = MassStorageFile

class SharedFile(MassStorageFile):

    ''' SharedFile. Special case of MassStorage for locally accessible fs through the standard lsb commands. '''
    _schema = MassStorageFile._schema.inherit_copy()
    _category = 'gangafiles'
    _name = 'SharedFile'

    # Copied from MassStorageFile to keep interface
    def __init__(self, namePattern='', localDir='', **kwds):
        """
        SharedFile construction
        Args:
            namePattern (str): is the pattern of the output file that has to be written into mass storage
            localDir (str): This is the optional local directory of a file to be uploaded to mass storage
        """
        if getConfig('Output')[_getName(self)]['uploadOptions']['path'] is None:
            logger.error("In order to use the SharedFile class you will need to define the path directory in your .gangarc")
            raise GangaException("In order to use the SharedFile class you will need to define the path directory in your .gangarc")

        super(SharedFile, self).__init__(namePattern, localDir, **kwds)

# add SharedFile objects to the configuration scope (i.e. it will be
# possible to write instatiate SharedFile() objects via config file)
Ganga.Utility.Config.config_scope['SharedFile'] = SharedFile

