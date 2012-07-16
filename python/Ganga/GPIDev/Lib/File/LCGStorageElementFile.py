
################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: LCGStorageElementFile.py,v 0.1 2011-02-12 15:40:00 idzhunov Exp $
################################################################################

from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig 
import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

from OutputSandboxFile import OutputSandboxFile

import re
import os

class LCGStorageElementFile(OutputSandboxFile):
    """LCGStorageElementFile represents a class marking an output file to be written into LCG SE
    """
    lcgSEConfig = getConfig('Output')['LCGStorageElementFile']['uploadOptions']

    _schema = Schema(Version(1,1), {
        'namePattern'        : SimpleItem(defvalue="",doc='pattern of the file name'),
        'joboutputdir': SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
        'se'          : SimpleItem(defvalue=lcgSEConfig['dest_SRM'], copyable=1, doc='the LCG SE hostname'),
        'se_type'     : SimpleItem(defvalue='', copyable=1, doc='the LCG SE type'),
        'se_rpath'    : SimpleItem(defvalue='', copyable=1, doc='the relative path to the VO directory on the SE'),
        'lfc_host'    : SimpleItem(defvalue=lcgSEConfig['LFC_HOST'], copyable=1, doc='the LCG LFC hostname'),
        'srm_token'   : SimpleItem(defvalue='', copyable=1, doc='the SRM space token, meaningful only when se_type is set to srmv2'),
        'SURL'        : SimpleItem(defvalue='', copyable=1, doc='the LCG SE SURL'),
        'port'        : SimpleItem(defvalue='', copyable=1, doc='the LCG SE port'),
        'locations' : SimpleItem(defvalue=[],typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
        'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')})
    _category = 'outputfiles'
    _name = "LCGStorageElementFile"
    _exportmethods = [ "location" , "setLocation" , "get" , "getUploadCmd"]

    def __init__(self,namePattern='', **kwds):
        """ namePattern is the pattern of the output file that has to be written into LCG SE
        """
        super(LCGStorageElementFile, self).__init__(namePattern, **kwds)

        self.locations = []

    def __setattr__(self, attr, value):
        if attr == 'se_type' and value not in ['','srmv1','srmv2','se']:
            raise AttributeError('invalid se_type: %s' % value)
        super(LCGStorageElementFile,self).__setattr__(attr, value)

    def __construct__(self,args):
        super(LCGStorageElementFile,self).__construct__(args)

            
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

        postprocessLocationsPath = os.path.join(self.joboutputdir, getConfig('Output')['PostProcessLocationsFileName'])
        if not os.path.exists(postprocessLocationsPath):
            return

        postprocesslocations = open(postprocessLocationsPath, 'r')
        
        for line in postprocesslocations.readlines():
                
            if line.strip() == '':      
                continue

            if line.startswith('lcgse'):

                lcgSEUpload = line.strip()
                searchPattern = 'lcgse %s' % self.namePattern

                if lcgSEUpload.startswith(searchPattern):
                    guid = lcgSEUpload[lcgSEUpload.find('->')+2:]
                    if guid not in self.locations:
                        self.locations.append(guid)

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

        os.environ['LFC_HOST'] = self.lfc_host

        fileName = self.namePattern

        if self.compressed:
            fileName = '%s.gz' % self.namePattern          

        for currentFile in glob.glob(os.path.join(self.joboutputdir, fileName)):
            cmd = self.getUploadCmd()
            cmd = cmd.replace('filename', currentFile)
            cmd = cmd + ' file:%s' % currentFile

            (exitcode, mystdout, mystderr) = self.execSyscmdSubprocess(cmd)
            if exitcode == 0:
                
                match = re.search('(guid:\S+)',mystdout)
                if match:
                    location = mystdout.strip()
                    if location not in self.locations:
                        self.locations.append(location) 

                #remove file from output
                os.system('rm %s' % os.path.join(self.joboutputdir, currentFile))

            else:
                logger.warning('cmd %s failed with error : %s' % (cmd, mystderr))       
                                        
    
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
        
###INDENT###    guidResults = []

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
###INDENT###                guidResults.append(mystdout)

###INDENT###            #remove file from output dir
###INDENT###            os.system('rm %s' % currentFile)
###INDENT###        else:
###INDENT###            printError('cmd %s failed' % cmd + os.linesep + mystderr)   

###INDENT###    return guidResults    

###INDENT###for lcgseItem in ###LCGCOMMANDS###:
###INDENT###    guids = uploadToSE(lcgseItem)
###INDENT###    for guid in guids:
###INDENT###        ###POSTPROCESSLOCATIONSFP###.write('%s->%s\\n' % (lcgseItem, guid)) 
"""

        script = script.replace('###LCGCOMMANDS###', str(lcgCommands))
        script = script.replace('###PATTERNSTOZIP###', str(patternsToZip))
        script = script.replace('###INDENT###', indent)
        script = script.replace('###POSTPROCESSLOCATIONSFP###', postProcessLocationsFP)

        return script   

    def get(self, dir):
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

        if not os.path.isdir(dir):
            print "%s is not a valid directory.... " % dir
            return

        #set lfc host
        os.environ['LFC_HOST'] = self.lfc_host

        vo = getConfig('LCG')['VirtualOrganisation']  

        for location in self.locations:
            destFileName = os.path.join(dir, location[-10:])
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
