################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: MassStorageFile.py,v 0.1 2011-11-09 15:40:00 idzhunov Exp $
################################################################################
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig
from Ganga.GPIDev.Base.Proxy import GPIProxyObjectFactory

from IOutputFile import IOutputFile

import re
import os
import copy

regex = re.compile('[*?\[\]]')

class MassStorageFile(IOutputFile):
    """MassStorageFile represents a class marking a file to be written into mass storage (like Castor at CERN)
    """
    _schema = Schema(Version(1,1), {'namePattern': SimpleItem(defvalue="",doc='pattern of the file name'),
                                    'localDir': SimpleItem(defvalue="",copyable=1,doc='local dir where the file is stored, used from get and put methods'),        
                                    'joboutputdir': SimpleItem(defvalue="",doc='outputdir of the job with which the outputsandbox file object is associated'),
                                    'locations' : SimpleItem(defvalue=[],copyable=1,typelist=['str'],sequence=1,doc="list of locations where the outputfiles are uploaded"),
                                    'outputfilenameformat' : SimpleItem(defvalue=None, typelist=['str', 'type(None)'],protected=0,doc="keyword path to where the output should be uploaded, i.e. /some/path/here/{jid}/{sjid}/{fname}, if this field is not set, the output will go in {jid}/{sjid}/{fname} or in {jid}/{fname} depending on whether the job is split or not"),
                                    'subfiles'      : ComponentItem(category='outputfiles',defvalue=[], hidden=1, typelist=['Ganga.GPIDev.Lib.File.MassStorageFile'], sequence=1, copyable=0, doc="collected files from the wildcard namePattern"),
                                    'failureReason' : SimpleItem(defvalue="",protected=1,copyable=0,doc='reason for the upload failure'),
                                    'compressed' : SimpleItem(defvalue=False, typelist=['bool'],protected=0,doc='wheather the output file should be compressed before sending somewhere')
                                        })

    _category = 'outputfiles'
    _name = "MassStorageFile"
    _exportmethods = [ "location" , "get" , "put" , "setLocation" ]
        
    def __init__(self,namePattern='', localDir='', **kwds):
        """ namePattern is the pattern of the output file that has to be written into mass storage
        """
        super(MassStorageFile, self).__init__()
        self.namePattern = namePattern
        self.localDir = localDir
        self.locations = []

    def __construct__(self,args):
        if len(args) == 1 and type(args[0]) == type(''):
            self.namePattern = args[0]
        elif len(args) == 2 and type(args[0]) == type('') and type(args[1]) == type(''):
            self.namePattern = args[0]
            self.localDir = args[1]     
            
    def _on_attribute__set__(self, obj_type, attrib_name):
        r = copy.deepcopy(self)
        if obj_type.__class__.__name__ == 'Job' and attrib_name == 'outputfiles':
            r.locations=[]
            r.localDir=''
            r.failureReason=''
        return r

    def __repr__(self):
        """Get the representation of the file."""

        return "MassStorageFile(namePattern='%s')"% self.namePattern
    

    def setLocation(self):
        """
        Sets the location of output files that were uploaded to mass storage from the WN
        """
        
        job = self.getJobObject()

        postprocessLocationsPath = os.path.join(job.outputdir, getConfig('Output')['PostProcessLocationsFileName'])
        if not os.path.exists(postprocessLocationsPath):
            return

        postprocesslocations = open(postprocessLocationsPath, 'r')

        def mass_line_processor(line, mass_file):
            lineParts = line.split(' ') 
            pattern = lineParts[1]
            outputPath = lineParts[2]   
            name = os.path.basename(outputPath).strip('.gz')

            if regex.search(mass_file.namePattern) is not None:
                if outputPath == 'ERROR':
                    logger.error("Failed to upload file to mass storage")
                    logger.error(line[line.find('ERROR')+5:])
                    d=MassStorageFile(namePattern=pattern)
                    d.compressed = mass_file.compressed
                    d.failureReason = line[line.find('ERROR')+5:]
                    mass_file.subfiles.append(GPIProxyObjectFactory(d))             
                else:
                    d=MassStorageFile(namePattern=name)
                    d.compressed = mass_file.compressed
                    d.outputfilenameformat = mass_file.outputfilenameformat
                    mass_file.subfiles.append(GPIProxyObjectFactory(d))
                    mass_line_processor(line, d)
            elif pattern == mass_file.namePattern:
                if outputPath == 'ERROR':
                    logger.error("Failed to upload file to mass storage")
                    logger.error(line[line.find('ERROR')+5:])
                    mass_file.failureReason = line[line.find('ERROR')+5:]
                    return
                mass_file.locations = [outputPath.strip('\n')]

        for line in postprocesslocations.readlines():
                
            if line.strip() == '':      
                continue
         
            if line.startswith('massstorage'):
                mass_line_processor(line.strip(), self)

        postprocesslocations.close()            
        
    def location(self):
        """
        Return list with the locations of the post processed files (if they were configured to upload the output somewhere)
        """
        return self.locations

    def get(self):
        """
        Retrieves locally all files matching this MassStorageFile object pattern
        """

        to_location = self.localDir

        if not os.path.isdir(self.localDir):
            if self._parent is not None:
                to_location = self.getJobObject().outputdir
            else:
                print "%s is not a valid directory.... Please set the localDir attribute" % self.localDir
                return

         
        cp_cmd = getConfig('Output')['MassStorageFile']['uploadOptions']['cp_cmd']  

        for location in self.locations:
            targetLocation = os.path.join(to_location, os.path.basename(location))      
            os.system('%s %s %s' % (cp_cmd, location, targetLocation))

    def getWNScriptDownloadCommand(self, indent):

        script = """\n

###INDENT###os.system(\'###CP_COMMAND###\')

"""     
        cp_cmd = '%s %s .' % (getConfig('Output')['MassStorageFile']['uploadOptions']['cp_cmd']  , self.locations[0])
        script = script.replace('###INDENT###', indent)
        script = script.replace('###CP_COMMAND###', cp_cmd)

        return script

    def put(self):
        """
        Creates and executes commands for file upload to mass storage (Castor), this method will
        be called on the client
        """     
        import glob
        import re

        sourceDir = ''

        #if used as a stand alone object
        if self._parent == None:
            if self.localDir == '':
                logger.warning('localDir attribute is empty, don\'t know from which dir to take the file' )
                return
            else:
                sourceDir = self.localDir

                (result, message) = self.validate()

                if result == False:
                    logger.warning(message)
                    return
                    
        else:
            job = self.getJobObject()
            sourceDir = job.outputdir
                
            #if there are subjobs, the put method will be called on every subjob
            #and will upload the resulted output file 
            if len (job.subjobs) > 0:
                return

        massStorageConfig = getConfig('Output')['MassStorageFile']['uploadOptions']

        mkdir_cmd = massStorageConfig['mkdir_cmd']
        cp_cmd = massStorageConfig['cp_cmd']
        ls_cmd = massStorageConfig['ls_cmd']
        massStoragePath = massStorageConfig['path']

        #create the last directory (if not exist) from the config path
        pathToDirName = os.path.dirname(massStoragePath)
        dirName = os.path.basename(massStoragePath)

        (exitcode, mystdout, mystderr) = self.execSyscmdSubprocess('%s %s' % (ls_cmd, pathToDirName))
        if exitcode != 0:
            self.handleUploadFailure(mystderr)
            return

        directoryExists = False 
        for directory in mystdout.split('\n'):
            if directory.strip() == dirName:
                directoryExists = True
                break

        if not directoryExists:
            (exitcode, mystdout, mystderr) = self.execSyscmdSubprocess('%s %s' % (mkdir_cmd, massStoragePath))
            if exitcode != 0:
                self.handleUploadFailure(mystderr)
                return

        #the folder part of self.outputfilenameformat
        folderStructure = ''
        #the file name part of self.outputfilenameformat
        filenameStructure = ''

        if self._parent != None:
            jobfqid = self.getJobObject().fqid
        
            jobid = jobfqid
            subjobid = ''

            if (jobfqid.find('.') > -1):
                jobid = jobfqid.split('.')[0]
                subjobid = jobfqid.split('.')[1]
            
            if self.outputfilenameformat == None:
                filenameStructure = '{fname}'
                #create jid/sjid directories
                folderStructure = jobid
                if subjobid != '':
                    folderStructure = os.path.join(jobid, subjobid)
                    
            else:
                filenameStructure = os.path.basename(self.outputfilenameformat)
                filenameStructure = filenameStructure.replace('{jid}', jobid)

                folderStructure = os.path.dirname(self.outputfilenameformat)
                folderStructure = folderStructure.replace('{jid}', jobid)

                if subjobid != '':
                    filenameStructure = filenameStructure.replace('{sjid}', subjobid)
                    folderStructure = folderStructure.replace('{sjid}', subjobid)
        else:
            if self.outputfilenameformat != None:
                folderStructure = os.path.dirname(self.outputfilenameformat)
                filenameStructure = os.path.basename(self.outputfilenameformat)
            else:
                filenameStructure = '{fname}'
        
        #create the folder structure
        if folderStructure != '':
                
            folderStructure = folderStructure.strip('/')
            massStoragePath = os.path.join(massStoragePath, folderStructure)
            command = '%s -p %s' % (mkdir_cmd, massStoragePath)
            (exitcode, mystdout, mystderr) = self.execSyscmdSubprocess(command)
            if exitcode != 0:
                self.handleUploadFailure(mystderr)
                return

        #here filenameStructure has replaced jid and sjid if any, and only not replaced keyword is fname    
        fileName = self.namePattern
        if self.compressed:
            fileName = '%s.gz' % self.namePattern 

        if regex.search(fileName) is not None:      
            for currentFile in glob.glob(os.path.join(sourceDir, fileName)):
                finalFilename = filenameStructure.replace('{fname}', os.path.basename(currentFile))
                (exitcode, mystdout, mystderr) = self.execSyscmdSubprocess('%s %s %s' % (cp_cmd, currentFile, os.path.join(massStoragePath, finalFilename)))

                d=MassStorageFile(namePattern=os.path.basename(currentFile))
                d.compressed = self.compressed

                if exitcode != 0:
                    self.handleUploadFailure(mystderr)
                else:
                    logger.info('%s successfully uploaded to mass storage' % currentFile)              
                    d.locations = os.path.join(massStoragePath, os.path.basename(finalFilename))

                    ## Alex removed this as more general approach in job.py after put() is called
                    #remove file from output dir if this object is attached to a job
                    #if self._parent != None:
                    #    os.system('rm %s' % os.path.join(sourceDir, currentFile))

                self.subfiles.append(GPIProxyObjectFactory(d))
        else:
            currentFile = os.path.join(sourceDir, fileName)
            finalFilename = filenameStructure.replace('{fname}', os.path.basename(currentFile))
            (exitcode, mystdout, mystderr) = self.execSyscmdSubprocess('%s %s %s' % (cp_cmd, currentFile, os.path.join(massStoragePath, finalFilename)))
            if exitcode != 0:
                self.handleUploadFailure(mystderr)
            else:
                logger.info('%s successfully uploaded to mass storage' % currentFile)              
                location = os.path.join(massStoragePath, os.path.basename(finalFilename))
                if location not in self.locations:
                    self.locations.append(location)         

                ## Alex removed this as more general approach in job.py after put() is called
                #remove file from output dir if this object is attached to a job
                #if self._parent != None:
                #    os.system('rm %s' % os.path.join(sourceDir, currentFile))

    def validate(self):
        
        #if the user has set outputfilenameformat, validate for presence of
        #jid, sjid and fname keywords depending on job type - split or non-split
        if self.outputfilenameformat != None:

            searchFor = ['{fname}']
            isJob = False
            isSplitJob = False

            if self._parent != None:

                isJob = True  

                if (self.getJobObject().splitter != None):

                    isSplitJob = True
                    searchFor.append('{sjid}')    

            missingKeywords = []
            
            for item in searchFor:
                if self.outputfilenameformat.find(item) == -1:
                    missingKeywords.append(item)

            if len(missingKeywords):
                return (False, 'Error in MassStorageFile.outputfilenameformat field : missing keywords %s ' % ','.join(missingKeywords))
                
            if isSplitJob == False and self.outputfilenameformat.find('{sjid}') > -1:
                return (False, 'Error in MassStorageFile.outputfilenameformat field :  job is non-split, but {\'sjid\'} keyword found')

            if isJob == False and self.outputfilenameformat.find('{sjid}') > -1:
                return (False, 'Error in MassStorageFile.outputfilenameformat field :  no parent job, but {\'sjid\'} keyword found')

            if isJob == False and self.outputfilenameformat.find('{jid}') > -1:
                return (False, 'Error in MassStorageFile.outputfilenameformat field :  no parent job, but {\'jid\'} keyword found')

            invalidUnixChars = ['"', ' ']
            test = self.outputfilenameformat.replace('{jid}', 'a').replace('{sjid}', 'b').replace('{fname}', 'c')
            
            for invalidUnixChar in invalidUnixChars:
                if test.find(invalidUnixChar) > -1:
                    return (False, 'Error in MassStorageFile.outputfilenameformat field :  invalid char %s found' % invalidUnixChar)    

        return (True, '')

    def handleUploadFailure(self, error):
            
        self.failureReason = error
        if self._parent != None:
            logger.error("Job %s failed. One of the job.outputfiles couldn't be uploaded because of %s" % (str(self._parent.fqid), self.failureReason))
        else:
            logger.error("The file can't be uploaded because of %s" % (self.failureReason))


    def getWNInjectedScript(self, outputFiles, indent, patternsToZip, postProcessLocationsFP):

        """
        Returns script that have to be injected in the jobscript for postprocessing on the WN
        """        
        massStorageCommands = []
      
        massStorageConfig = getConfig('Output')['MassStorageFile']['uploadOptions']  

        for outputFile in outputFiles:

            outputfilenameformat = 'None'
            if outputFile.outputfilenameformat != None and outputFile.outputfilenameformat != '':
                outputfilenameformat = outputFile.outputfilenameformat

            massStorageCommands.append(['massstorage',outputFile.namePattern , outputfilenameformat, massStorageConfig['mkdir_cmd'],  massStorageConfig['cp_cmd'], massStorageConfig['ls_cmd'], massStorageConfig['path']])

                
        script = """\n
###INDENT###import subprocess
###INDENT####system command executor with subprocess
###INDENT###def execSyscmdSubprocessAndReturnOutputMAS(cmd):

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
        
###INDENT###for massStorageList in ###MASSSTORAGECOMMANDS###:

###INDENT###    filenameWildChar = massStorageList[1]
###INDENT###    outputfilenameformat = massStorageList[2]
###INDENT###    cm_mkdir = massStorageList[3]
###INDENT###    cm_cp = massStorageList[4]
###INDENT###    cm_ls = massStorageList[5]
###INDENT###    path = massStorageList[6]

###INDENT###    pathToDirName = os.path.dirname(path)
###INDENT###    dirName = os.path.basename(path)

###INDENT###    (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutputMAS('%s %s' % (cm_ls, pathToDirName))
###INDENT###    if exitcode != 0:
###INDENT###        ###POSTPROCESSLOCATIONSFP###.write('massstorage %s ERROR %s\\n' % (filenameWildChar, mystderr))
###INDENT###        continue

###INDENT###    directoryExists = False 
###INDENT###    for directory in mystdout.split('\\n'):
###INDENT###        if directory.strip() == dirName:
###INDENT###            directoryExists = True
###INDENT###            break

###INDENT###    if not directoryExists:
###INDENT###        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutputMAS('%s %s' % (cm_mkdir, path))
###INDENT###        if exitcode != 0:
###INDENT###            printError('Error while executing %s %s command, check if the ganga user has rights for creating ###INDENT###directories in this folder' % (cm_mkdir, path) + os.linesep + mystderr)
###INDENT###            ###POSTPROCESSLOCATIONSFP###.write('massstorage %s ERROR %s\\n' % (filenameWildChar, mystderr))
###INDENT###            continue

###INDENT###    folderStructure = ''
###INDENT###    filenameStructure = ''
###INDENT###
###INDENT###    jobid = '###JOBDIR###'
###INDENT###    subjobid = '###SUBJOBDIR###'
###INDENT###        
###INDENT###    if outputfilenameformat == 'None':
###INDENT###        filenameStructure = '{fname}'
###INDENT###        folderStructure = jobid
###INDENT###        if subjobid != '':
###INDENT###            folderStructure = os.path.join(jobid, subjobid)
###INDENT###                
###INDENT###    else:
###INDENT###        filenameStructure = os.path.basename(outputfilenameformat)
###INDENT###        filenameStructure = filenameStructure.replace('{jid}', jobid)
###INDENT###
###INDENT###        folderStructure = os.path.dirname(outputfilenameformat)
###INDENT###        folderStructure = folderStructure.replace('{jid}', jobid)
###INDENT###
###INDENT###        if subjobid != '':
###INDENT###            filenameStructure = filenameStructure.replace('{sjid}', subjobid)
###INDENT###            folderStructure = folderStructure.replace('{sjid}', subjobid)
###INDENT###
###INDENT###    if folderStructure != '':               
###INDENT###        folderStructure = folderStructure.strip('/')
###INDENT###        path = os.path.join(path, folderStructure)
###INDENT###        command = '%s -p %s' % (cm_mkdir, path)
###INDENT###        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutputMAS(command)
###INDENT###        if exitcode != 0:
###INDENT###            printError('Error while executing %s  command, check if the ganga user has rights for creating ###INDENT###directories in this folder' % command + os.linesep + mystderr)
###INDENT###            ###POSTPROCESSLOCATIONSFP###.write('massstorage %s ERROR %s\\n' % (filenameWildChar, mystderr))
###INDENT###            continue


###INDENT###    filenameWildCharZipped = filenameWildChar
###INDENT###    if filenameWildChar in ###PATTERNSTOZIP###:
###INDENT###        filenameWildCharZipped = '%s.gz' % filenameWildChar

###INDENT###    for currentFile in glob.glob(os.path.join(os.getcwd(),filenameWildCharZipped)):
###INDENT###        currentFileBaseName = os.path.basename(currentFile)
###INDENT###        finalFilename = filenameStructure.replace('{fname}', currentFileBaseName)
###INDENT###        (exitcode, mystdout, mystderr) = execSyscmdSubprocessAndReturnOutputMAS('%s %s %s' % (cm_cp, currentFile, os.path.join(path, finalFilename)))
###INDENT###        if exitcode != 0:
###INDENT###            printError('Error while executing %s %s %s command, check if the ganga user has rights for uploading ###INDENT###files to this mass storage folder' % (cm_cp, currentFile, os.path.join(path, finalFilename)) + os.linesep ###INDENT### + mystderr)
###INDENT###            ###POSTPROCESSLOCATIONSFP###.write('massstorage %s ERROR %s\\n' % (filenameWildChar, mystderr))
###INDENT###        else:
###INDENT###            ###POSTPROCESSLOCATIONSFP###.write('massstorage %s %s\\n' % (filenameWildChar, os.path.join(path, finalFilename)))
###INDENT###            #remove file from output dir
###INDENT###            os.system('rm %s' % currentFile)
"""

        script = script.replace('###MASSSTORAGECOMMANDS###', repr(massStorageCommands))
        script = script.replace('###PATTERNSTOZIP###', str(patternsToZip))
        script = script.replace('###INDENT###', indent)
        script = script.replace('###POSTPROCESSLOCATIONSFP###', postProcessLocationsFP)

        jobfqid = self.getJobObject().fqid
        
        jobid = jobfqid
        subjobid = ''

        if (jobfqid.find('.') > -1):
            jobid = jobfqid.split('.')[0]
            subjobid = jobfqid.split('.')[1]
        
        script = script.replace('###FULLJOBDIR###', str(jobfqid.replace('.', os.path.sep)))
        script = script.replace('###JOBDIR###', str(jobid))
        script = script.replace('###SUBJOBDIR###', str(subjobid))

        return script   

# add MassStorageFile objects to the configuration scope (i.e. it will be possible to write instatiate MassStorageFile() objects via config file)
import Ganga.Utility.Config
Ganga.Utility.Config.config_scope['MassStorageFile'] = MassStorageFile
