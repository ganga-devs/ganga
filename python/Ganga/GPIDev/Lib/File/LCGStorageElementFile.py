
################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LCGStorageElementFile.py,v 0.1 2011-02-12 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig 
import Ganga.Utility.logging
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory
logger = Ganga.Utility.logging.getLogger()

from IOutputFile import IOutputFile

import re
import os

regex = re.compile('[*?\[\]]')

class LCGStorageElementFile(IOutputFile):
    """LCGStorageElementFile represents a class marking an output file to be written into LCG SE
    """
    lcgSEConfig = getConfig('Output')['LCGStorageElementFile']['uploadOptions']

    _schema = Schema(Version(1,1), {
        'namePattern' : SimpleItem(defvalue="",doc='pattern of the file name'),
        'localDir'    : SimpleItem(defvalue="",copyable=0,doc='local dir where the file is stored, used from get and put methods'),    
        'joboutputdir': SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
        'se'          : SimpleItem(defvalue=lcgSEConfig['dest_SRM'], copyable=1, doc='the LCG SE hostname'),
        'se_type'     : SimpleItem(defvalue='', copyable=1, doc='the LCG SE type'),
        'se_rpath'    : SimpleItem(defvalue='', copyable=1, doc='the relative path to the VO directory on the SE'),
        'lfc_host'    : SimpleItem(defvalue=lcgSEConfig['LFC_HOST'], copyable=1, doc='the LCG LFC hostname'),
        'srm_token'   : SimpleItem(defvalue='', copyable=1, doc='the SRM space token, meaningful only when se_type is set to srmv2'),
        'SURL'        : SimpleItem(defvalue='', copyable=1, doc='the LCG SE SURL'),
        'port'        : SimpleItem(defvalue='', copyable=1, doc='the LCG SE port'),
        'locations'   : SimpleItem(defvalue=[],copyable=0,typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
        'subfiles'      : ComponentItem(category='outputfiles',defvalue=[], hidden=1, typelist=['Ganga.GPIDev.Lib.File.LCGStorageElementFile'], sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),
        'failureReason' : SimpleItem(defvalue="",copyable=0,doc='reason for the upload failure'),
        'compressed'  : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')})
    _category = 'outputfiles'
    _name = "LCGStorageElementFile"
    _exportmethods = [ "location" , "setLocation" , "get" , "put" , "getUploadCmd"]

    def __init__(self,namePattern='', localDir='', **kwds):
        """ namePattern is the pattern of the output file that has to be written into LCG SE
        """
        super(LCGStorageElementFile, self).__init__()
        self.namePattern = namePattern
        self.localDir = localDir

        self.locations = []

    def __setattr__(self, attr, value):
        if attr == 'se_type' and value not in ['','srmv1','srmv2','se']:
            raise AttributeError('invalid se_type: %s' % value)
        super(LCGStorageElementFile,self).__setattr__(attr, value)

    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            self.namePattern = args[0]
        elif len(args) == 2 and type(args[0]) == type('') and type(args[1]) == type(''):
            self.namePattern = args[0]
            self.localDir = args[1]     
            
    def __repr__(self):
        """Get the representation of the file."""

        return "LCGStorageElementFile(namePattern='%s')"% self.namePattern

    
    def __get_unique_fname__(self):
        '''gets an unique filename'''

        import random
        import time

        uuid = (str(random.uniform(0,100000000))+'-'+str(time.time())).replace('.','-')
        user = getConfig('Configuration')['user']

        fname = 'user.%s.%s' % (user, uuid)
        return fname
    
    def setLocation(self):
        """
        Sets the location of output files that were uploaded to lcg storage element from the WN
        """

        job = self.getJobObject()

        postprocessLocationsPath = os.path.join(job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])
        if not os.path.exists(postprocessLocationsPath):
            return

        postprocesslocations = open(postprocessLocationsPath, 'r')

        def lcgse_line_processor(line, lcgse_file):
            guid = line[line.find('->')+2:]
            pattern = line.split(' ')[1]
            name = line.split(' ')[2].strip('.gz')

            if regex.search(lcgse_file.namePattern) is not None:
                d=LCGStorageElementFile(namePattern=name)
                d.compressed = lcgse_file.compressed
                d.lfc_host = lcgse_file.lfc_host
                d.se = lcgse_file.se
                #todo copy also the other attributes
                lcgse_file.subfiles.append(GPIProxyObjectFactory(d))
                lcgse_line_processor(line, d)
            else:
                if guid.startswith('ERROR'):
                    logger.error("Failed to upload file to LSG SE")
                    logger.error(guid[6:])
                    lcgse_file.failureReason = guid[6:]
                    return
                lcgse_file.locations = guid

        for line in postprocesslocations.readlines():
                
            if line.strip() == '':      
                continue
         
            if line.startswith('lcgse'):
                lcgse_line_processor(line.strip(), self)

        postprocesslocations.close()
        
    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        return self.locations

    
    def getUploadCmd(self):

        vo = getConfig('LCG')['VirtualOrganisation']

        cmd = 'lcg-cr --vo %s ' % vo
        if self.se != '':
            cmd  = cmd + ' -d %s' % self.se
        if self.se_type == 'srmv2' and self.srm_token != '':
            cmd = cmd + ' -D srmv2 -s %s' % self.srm_token
          
        ## specify the physical location
        if self.se_rpath != '':
            cmd = cmd + ' -P %s/ganga.%s/filename' % ( self.se_rpath, self.__get_unique_fname__() )

        return cmd

    def put(self):
        """
        Executes the internally created command for file upload to LCG SE, this method will
        be called on the client
        """     
        import glob

        sourceDir = ''

        #if used as a stand alone object
        if self._parent == None:
            if self.localDir == '':
                logger.warning('localDir attribute is empty, don\'t know from which dir to take the file' )
                return
            else:
                sourceDir = self.localDir
        else:
            job = self.getJobObject()
            sourceDir = job.outputdir

        os.environ['LFC_HOST'] = self.lfc_host

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern          

        if regex.search(fileName) is not None:
            for currentFile in glob.glob(os.path.join(sourceDir, fileName)):
                cmd = self.getUploadCmd()
                cmd = cmd.replace('filename', currentFile)
                cmd = cmd + ' file:%s' % currentFile

                (exitcode, mystdout, mystderr) = self.execSyscmdSubprocess(cmd)

                d=LCGStorageElementFile(namePattern=os.path.basename(currentFile))
                d.compressed = self.compressed
                d.lfc_host = self.lfc_host
                d.se = self.se
                #todo copy also the other attributes

                if exitcode == 0:
                
                    match = re.search('(guid:\S+)',mystdout)
                    if match:
                        d.locations = mystdout.strip()

                    #remove file from output dir if this object is attached to a job
                    if self._parent != None:
                        os.system('rm %s' % os.path.join(sourceDir, currentFile))

                else:
                    d.failureReason = mystderr
                    if self._parent != None:
                        logger.error("Job %s failed. One of the job.outputfiles couldn't be uploaded because of %s" % (str(self._parent.fqid), self.failureReason))
                    else:
                        logger.error("The file can't be uploaded because of %s" % (self.failureReason))

                self.subfiles.append(GPIProxyObjectFactory(d))
                
        else:
            currentFile = os.path.join(sourceDir, fileName)
            cmd = self.getUploadCmd()
            cmd = cmd.replace('filename', currentFile)
            cmd = cmd + ' file:%s' % currentFile

            (exitcode, mystdout, mystderr) = self.execSyscmdSubprocess(cmd)

            if exitcode == 0:
                
                match = re.search('(guid:\S+)',mystdout)
                if match:       
                    self.locations = mystdout.strip()

                #remove file from output dir if this object is attached to a job
                if self._parent != None:
                    os.system('rm %s' % os.path.join(sourceDir, currentFile))

            else:
                self.failureReason = mystderr
                if self._parent != None:
                    logger.error("Job %s failed. One of the job.outputfiles couldn't be uploaded because of %s" % (str(self._parent.fqid), self.failureReason))
                else:
                    logger.error("The file can't be uploaded because of %s" % (self.failureReason))
            
    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):
        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """        
        lcgCommands = []

        for outputFile in outputFiles:
            lcgCommands.append('lcgse %s %s %s' % (outputFile.namePattern , outputFile.lfc_host,  outputFile.getUploadCmd()))
                
        script = """\n

###INDENT####system command executor with subprocess
###INDENT###def execSyscmdSubprocessAndReturnOutputLCG(cmd):

###INDENT###    exitcode = -999
###INDENT###    mystdout = ''
###INDENT###    mystderr = ''

###INDENT###    try:
###INDENT###        child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
###INDENT###        (mystdout, mystderr) = child.communicate()
###INDENT###        exitcode = child.returncode
###INDENT###    finally:
###INDENT###        pass

###INDENT###    return (exitcode, mystdout, mystderr)
        
###INDENT###def uploadToSE(lcgseItem):
        
###INDENT###    import re

###INDENT###    lcgseItems = lcgseItem.split(' ')

###INDENT###    filenameWildChar = lcgseItems[1]
###INDENT###    lfc_host = lcgseItems[2]

###INDENT###    cmd = lcgseItem[lcgseItem.find('lcg-cr'):]

###INDENT###    os.environ['LFC_HOST'] = lfc_host
        
###INDENT###    guidResults = {}

###INDENT###    if filenameWildChar in ###PATTERNSTOZIP###:
###INDENT###        filenameWildChar = '%s.gz' % filenameWildChar

###INDENT###    for currentFile in glob.glob(os.path.join(os.getcwd(), filenameWildChar)):
###INDENT###        cmd = lcgseItem[lcgseItem.find('lcg-cr'):]
###INDENT###        cmd = cmd.replace('filename', currentFile)
###INDENT###        cmd = cmd + ' file:%s' % currentFile
###INDENT###        printInfo(cmd)  
###INDENT###        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutputLCG(cmd)
###INDENT###        if exitcode == 0:
###INDENT###            printInfo('result from cmd %s is %s' % (cmd,str(mystdout)))
###INDENT###            match = re.search('(guid:\S+)',mystdout)
###INDENT###            if match:
###INDENT###                guidResults[mystdout] = os.path.basename(currentFile)

###INDENT###        else:
###INDENT###            guidResults['ERROR ' + mystderr] = ''
###INDENT###            printError('cmd %s failed' % cmd + os.linesep + mystderr)   

###INDENT###    return guidResults    

###INDENT###for lcgseItem in ###LCGCOMMANDS###:
###INDENT###    guids = uploadToSE(lcgseItem)
###INDENT###    for guid in guids.keys():
###INDENT###        ###POSTPROCESSLOCATIONSFP###.write('%s %s %s ->%s\\n' % (lcgseItem.split(' ')[0], lcgseItem.split(' ')[1], guids[guid], guid)) 

###INDENT####lets clear after us    
###INDENT###for lcgseItem in ###LCGCOMMANDS###:
###INDENT###    lcgseItems = lcgseItem.split(' ')

###INDENT###    filenameWildChar = lcgseItems[1]

###INDENT###    if filenameWildChar in ###PATTERNSTOZIP###:
###INDENT###        filenameWildChar = '%s.gz' % filenameWildChar

###INDENT###    for currentFile in glob.glob(os.path.join(os.getcwd(), filenameWildChar)):
###INDENT###        os.system('rm %s' % currentFile)
"""

        script = script.replace('###LCGCOMMANDS###', str(lcgCommands))
        script = script.replace('###PATTERNSTOZIP###', str(patternsToZip))
        script = script.replace('###INDENT###', indent)
        script = script.replace('###POSTPROCESSLOCATIONSFP###', postProcessLocationsFP)

        return script   

    def get(self):
        """
        Retrieves locally all files matching this LCGStorageElementFile object pattern
        """
        import subprocess
        
        # system command executor with subprocess
        def execSyscmdSubprocess(cmd):

            exitcode = -999
            mystdout = ''
            mystderr = ''

            try:
                child = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                (mystdout, mystderr) = child.communicate()
                exitcode = child.returncode
            finally:
                pass

            return (exitcode, mystdout, mystderr)

        from_location = self.localDir

        if not os.path.isdir(self.localDir):
            if self._parent is not None:
                from_location = self.getJobObject().outputdir
            else:
                print "%s is not a valid directory.... Please set the localDir attribute" % self.localDir
                return

        #set lfc host
        os.environ['LFC_HOST'] = self.lfc_host

        vo = getConfig('LCG')['VirtualOrganisation']  

        for location in self.locations:
            destFileName = os.path.join(from_location, location[-10:])
            cmd = 'lcg-cp --vo %s %s file:%s' % (vo, location, destFileName)
            (exitcode, mystdout, mystderr) = execSyscmdSubprocess(cmd)

            if exitcode == 0:
                print 'job output downloaded here %s' % destFileName
            else:
                print 'command %s failed to execute , reason for failure is %s' % (cmd, mystderr)
                print 'most probably you need to source the grid environment , set environment variable LFC_HOST to %s and try again with the lcg-cp command to download the job output' % self.lfc_host


# add LCGStorageElementFile objects to the configuration scope (i.e. it will be possible to write instatiate LCGStorageElementFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['LCGStorageElementFile'] = LCGStorageElementFile
