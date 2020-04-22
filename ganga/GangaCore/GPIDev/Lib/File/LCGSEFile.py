
##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LCGSEFile.py,v 0.1 2011-02-12 15:40:00 idzhunov Exp $
##########################################################################

from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem, ComponentItem

from GangaCore.Utility.Config import getConfig
import GangaCore.Utility.logging
from GangaCore.GPIDev.Base.Proxy import GPIProxyObjectFactory
logger = GangaCore.Utility.logging.getLogger()
from GangaCore.Utility.GridShell import getShell

from GangaCore.GPIDev.Adapters.IGangaFile import IGangaFile
from GangaCore.GPIDev.Base.Proxy import getName

from GangaCore.GPIDev.Credentials import require_credential, VomsProxy

import re
import os
import copy

regex = re.compile(r'[*?\[\]]')


def getLCGConfig():
    return getConfig('Output')['LCGSEFile']['uploadOptions']

class LCGSEFile(IGangaFile):

    """LCGSEFile represents a class marking an output file to be written into LCG SE
    """

    _schema = Schema(Version(1, 1), {
        'namePattern': SimpleItem(defvalue="", doc='pattern of the file name'),
        'localDir': SimpleItem(defvalue="", copyable=1, doc='local dir where the file is stored, used from get and put methods'),
        'joboutputdir': SimpleItem(defvalue="", doc='outputdir of the job with which the outputsandbox file object is associated'),
        'se': SimpleItem(defvalue=getLCGConfig()['dest_SRM'], copyable=1, doc='the LCG SE hostname'),
        'se_type': SimpleItem(defvalue='', copyable=1, doc='the LCG SE type'),
        'se_rpath': SimpleItem(defvalue='', copyable=1, doc='the relative path to the file from the VO directory on the SE'),
        'lfc_host': SimpleItem(defvalue=getLCGConfig()['LFC_HOST'], copyable=1, doc='the LCG LFC hostname'),
        'srm_token': SimpleItem(defvalue='', copyable=1, doc='the SRM space token, meaningful only when se_type is set to srmv2'),
        'SURL': SimpleItem(defvalue='', copyable=1, doc='the LCG SE SURL'),
        'port': SimpleItem(defvalue='', copyable=1, doc='the LCG SE port'),
        'locations': SimpleItem(defvalue=[], copyable=1, typelist=[str], sequence=1, doc="list of locations where the outputfiles were uploaded"),
        'subfiles': ComponentItem(category='gangafiles', defvalue=[], hidden=1, sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),
        'failureReason': SimpleItem(defvalue="", protected=1, copyable=0, doc='reason for the upload failure'),
        'compressed': SimpleItem(defvalue=False, typelist=[bool], protected=0, doc='wheather the output file should be compressed before sending somewhere'),
        'credential_requirements': ComponentItem('CredentialRequirement', defvalue='VomsProxy'),
    })
    _category = 'gangafiles'
    _name = "LCGSEFile"
    _exportmethods = ["location", "setLocation", "get", "put", "getUploadCmd"]

    def __init__(self, namePattern='', localDir='', **kwds):
        """ namePattern is the pattern of the output file that has to be written into LCG SE
        """
        super(LCGSEFile, self).__init__()
        self.namePattern = namePattern
        self.localDir = localDir

        self.locations = []

    def __setattr__(self, attr, value):
        if attr == 'se_type' and value not in ['', 'srmv1', 'srmv2', 'se']:
            raise AttributeError('invalid se_type: %s' % value)
        super(LCGSEFile, self).__setattr__(attr, value)

    def _on_attribute__set__(self, obj_type, attrib_name):
        r = copy.deepcopy(self)
        if getName(obj_type) == 'Job' and attrib_name == 'outputfiles':
            r.locations = []
            r.localDir = ''
            r.failureReason = ''
        return r

    def __repr__(self):
        """Get the representation of the file."""

        return "LCGSEFile(namePattern='%s')" % self.namePattern

    def __get_unique_fname__(self):
        '''gets an unique filename'''

        import random
        import time

        uuid = (str(random.uniform(0, 100000000)) +
                '-' + str(time.time())).replace('.', '-')
        user = getConfig('Configuration')['user']

        fname = 'user.%s.%s' % (user, uuid)
        return fname

    def setLocation(self):
        """
        Sets the location of output files that were uploaded to lcg storage element from the WN
        """

        job = self.getJobObject()

        postprocessLocationsPath = os.path.join(
            job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])
        if not os.path.exists(postprocessLocationsPath):
            return

        def lcgse_line_processor(line, lcgse_file):
            guid = line[line.find('->') + 2:]
            pattern = line.split(' ')[1]
            name = line.split(' ')[2].strip('.gz')

            if regex.search(lcgse_file.namePattern) is not None:
                d = LCGSEFile(namePattern=name)
                d.compressed = lcgse_file.compressed
                d.lfc_host = lcgse_file.lfc_host
                d.se = lcgse_file.se
                # todo copy also the other attributes
                lcgse_file.subfiles.append(GPIProxyObjectFactory(d))
                lcgse_line_processor(line, d)
            elif pattern == lcgse_file.namePattern:
                if guid.startswith('ERROR'):
                    logger.error("Failed to upload file to LCG SE")
                    logger.error(guid[6:])
                    lcgse_file.failureReason = guid[6:]
                    return
                lcgse_file.locations = guid

        for line in open(postprocessLocationsPath, 'r'):

            if line.strip() == '':
                continue

            if line.startswith('lcgse'):
                lcgse_line_processor(line.strip(), self)

    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        return self.locations

    def getUploadCmd(self):

        vo = self.credential_requirements.vo

        cmd = 'lcg-cr --vo %s ' % vo
        if self.se != '':
            cmd = cmd + ' -d %s' % self.se
        if self.se_type == 'srmv2' and self.srm_token != '':
            cmd = cmd + ' -D srmv2 -s %s' % self.srm_token

        # specify the physical location
        if self.se_rpath != '':
            cmd = cmd + \
                ' -P %s/ganga.%s/filename' % (self.se_rpath,
                                              self.__get_unique_fname__())

        return cmd

    @require_credential
    def put(self):
        """
        Executes the internally created command for file upload to LCG SE, this method will
        be called on the client
        """
        import glob

        sourceDir = ''

        # if used as a stand alone object
        if self._getParent() is None:
            if self.localDir == '':
                logger.warning(
                    'localDir attribute is empty, don\'t know from which dir to take the file')
                return
            else:
                sourceDir = self.localDir
        else:
            job = self.getJobObject()
            sourceDir = job.outputdir
        import os
        os.environ['LFC_HOST'] = self.lfc_host

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern

        if regex.search(fileName) is not None:
            for currentFile in glob.glob(os.path.join(sourceDir, fileName)):
                cmd = self.getUploadCmd()
                cmd = cmd.replace('filename', currentFile)
                cmd = cmd + ' file:%s' % currentFile

                (exitcode, output, m) = getShell(self.credential_requirements).cmd1(
                    cmd, capture_stderr=True)

                d = LCGSEFile(namePattern=os.path.basename(currentFile))
                d.compressed = self.compressed
                d.lfc_host = self.lfc_host
                d.se = self.se
                # todo copy also the other attributes

                if exitcode == 0:

                    match = re.search(r'(guid:\S+)', output)
                    if match:
                        d.locations = output.strip()

                    # Alex removed this as more general approach in job.py after put() is called
                    # remove file from output dir if this object is attached to a job
                    # if self._getParent() is not None:
                    #    os.system('rm %s' % os.path.join(sourceDir, currentFile))

                else:
                    d.failureReason = output
                    if self._getParent() is not None:
                        logger.error("Job %s failed. One of the job.outputfiles couldn't be uploaded because of %s" % (
                            str(self._getParent().fqid), self.failureReason))
                    else:
                        logger.error(
                            "The file can't be uploaded because of %s" % (self.failureReason))

                self.subfiles.append(GPIProxyObjectFactory(d))

        else:
            logger.debug("sourceDir: %s" % sourceDir)
            logger.debug("fileName: %s" % fileName)
            currentFile = os.path.join(sourceDir, fileName)
            import os.path
            if os.path.isfile(currentFile):
                logger.debug("currentFile: %s exists!" % currentFile)
            else:
                logger.debug("currentFile: %s DOES NOT exist!" % currentFile)

            cmd = self.getUploadCmd()
            cmd = cmd.replace('filename', currentFile)
            cmd = cmd + ' file:%s' % currentFile

            logger.debug("cmd is: %s" % cmd)

            (exitcode, output, m) = getShell(self.credential_requirements).cmd1(cmd, capture_stderr=True)

            if exitcode == 0:

                match = re.search(r'(guid:\S+)', output)
                if match:
                    self.locations = output.strip()

                # Alex removed this as more general approach in job.py after put() is called
                # remove file from output dir if this object is attached to a job
                # if self._getParent() is not None:
                #    os.system('rm %s' % os.path.join(sourceDir, currentFile))

            else:
                self.failureReason = output
                if self._getParent() is not None:
                    logger.error("Job %s failed. One of the job.outputfiles couldn't be uploaded because of %s" % (
                        str(self._getParent().fqid), self.failureReason))
                else:
                    logger.error(
                        "The file can't be uploaded because of %s" % (self.failureReason))

    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """
        lcgCommands = []

        for outputFile in outputFiles:
            lcgCommands.append('lcgse %s %s %s' % (
                outputFile.namePattern, outputFile.lfc_host,  outputFile.getUploadCmd()))
            logger.debug("OutputFile (%s) cmd for WN script is: %s" %
                         (outputFile.namePattern, outputFile.getUploadCmd()))

        import inspect
        script_location = os.path.join(os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe()))),
                                        'scripts/LCGSEFileWNScript.py.template')

        from GangaCore.GPIDev.Lib.File import FileUtils
        script = FileUtils.loadScript(script_location, '###INDENT###')

        script = script.replace('###LCGCOMMANDS###', str(lcgCommands))
        script = script.replace('###PATTERNSTOZIP###', str(patternsToZip))
        script = script.replace('###INDENT###', indent)
        script = script.replace('###POSTPROCESSLOCATIONSFP###', postProcessLocationsFP)

        return script

    @require_credential
    def internalCopyTo(self, targetPath):
        """
        Retrieves locally all files matching this LCGSEFile object pattern
        Args:
            targetPath (str): Target path where the file is copied to
        """
        to_location = targetPath

        # set lfc host
        os.environ['LFC_HOST'] = self.lfc_host

        vo = self.credential_requirements.vo

        for location in self.locations:
            destFileName = os.path.join(to_location, self.namePattern)
            cmd = 'lcg-cp --vo {vo} {remote_path} file:{local_path}'.format(vo=vo, remote_path=location, local_path=destFileName)
            (exitcode, output, m) = getShell(self.credential_requirements).cmd1(cmd, capture_stderr=True)

            if exitcode != 0:
                logger.error('command %s failed to execute , reason for failure is %s' % (cmd, output))

    def getWNScriptDownloadCommand(self, indent):

        script = """\n

###INDENT###os.environ['LFC_HOST'] = '###LFC_HOST###'
###INDENT###cwDir = os.getcwd()
###INDENT###dwnCmd = 'lcg-cp --vo ###VO### lfn:/grid/###VO###/###LOCATION###/###NAMEPATTERN### file:%s' % os.path.join(cwDir, '###NAMEPATTERN###')
###INDENT###os.system(dwnCmd)
"""

        script = script.replace('###INDENT###', indent)
        script = script.replace('###LFC_HOST###', self.lfc_host)
        script = script.replace(
            '###VO###', self.credential_requirements.vo)
        script = script.replace('###LOCATION###', self.se_rpath)
        script = script.replace('###NAMEPATTERN###', self.namePattern)

        return script

    @require_credential
    def processWildcardMatches(self):
        if self.subfiles:
            return self.subfiles

        from fnmatch import fnmatch

        if regex.search(self.namePattern):
            #TODO namePattern shouldn't contain slashes and se_rpath should not contain wildcards
            cmd = 'lcg-ls lfn:/grid/{vo}/{se_rpath}'.format(vo=self.credential_requirements.vo, se_rpath=self.se_rpath)
            exitcode,output,m = getShell(self.credential_requirements).cmd1(cmd, capture_stderr=True)

            for filename in output.split('\n'):
                if fnmatch(filename, self.namePattern):
                    subfile = LCGSEFile(namePattern=filename)
                    subfile.se_rpath = self.se_rpath
                    subfile.lfc_host = self.lfc_host

                    self.subfiles.append(GPIProxyObjectFactory(subfile))

# add LCGSEFile objects to the configuration scope (i.e. it will be
# possible to write instatiate LCGSEFile() objects via config file)
import GangaCore.Utility.Config
GangaCore.Utility.Config.config_scope['LCGSEFile'] = LCGSEFile
