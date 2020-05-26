################################################################################
# Ganga Project. http://cern.ch/ganga
#
################################################################################

from GangaCore.GPIDev.Base import GangaObject
from GangaCore.GPIDev.Adapters.IPostProcessor import PostProcessException
from GangaCore.GPIDev.Adapters.IChecker import IChecker, IFileChecker
from GangaCore.GPIDev.Base.Proxy import GPIProxyObject
from GangaCore.GPIDev.Schema import ComponentItem, FileItem, Schema, SimpleItem, Version
from GangaCore.Utility.Plugin import allPlugins
from GangaCore.Utility.logging import getLogger

import subprocess
import copy
import os
import string
import shutil

# Simon's post_status - communicates to processing DB
from . import post_status
import urllib.request, urllib.error, urllib.parse

logger = getLogger()

class FileCheckeR(IFileChecker):
    """
    Checks if string is in file.
    self.searchStrings are the search strings you would like to check for.
    self.files are the files you would like to check.
    self.failIfFound (default = True) decides whether to fail the job if the string is found. If you set this to false the job will fail if the string *isnt* found.
    self.filesMustExist toggles whether to fail the job if the specified file doesn't exist (default is True).
    """
    _schema = IFileChecker._schema.inherit_copy()
    _schema.datadict['searchStrings'] = SimpleItem(defvalue = [], doc='String to search for')
    _schema.datadict['failIfFound'] = SimpleItem(True, doc='Toggle whether job fails if string is found or not found.')
    _category = 'postprocessor'
    _name = 'FileCheckeR'
    _exportmethods = ['check']


    def check(self,job):
        """
        Check that a string is in a file, takes the job object as input.
        """

        if not len(self.searchStrings):
            raise PostProcessException('No searchStrings specified, FileCheckeR will do nothing!')
        filepaths = self.findFiles(job)
        if not len(filepaths):
            raise PostProcessException('None of the files to check exist, FileCheckeR will do nothing!') 
        for filepath in filepaths:
            for searchString in self.searchStrings:
                grepoutput = subprocess.getoutput('grep "%s" %s' % (searchString,filepath))
                if len(grepoutput) and self.failIfFound is True:            
                    logger.info('The string %s has been found in file %s, FileCheckeR will fail job(%s)',searchString,filepath,job.fqid)
                    return self.failure
                if not len(grepoutput) and self.failIfFound is False:            
                    logger.info('The string %s has not been found in file %s, FileCheckeR will fail job(%s)',searchString,filepath,job.fqid)
                    return self.failure
        return self.result 


class ND280Kin_Checker(IFileChecker):
    """
    o Checks .log file (not impl. yet)
    o Checks if .kin file is present and non-zero
    o Moves output files to their destinations
    """
    _schema = IFileChecker._schema.inherit_copy()
    _schema.datadict['prfx'] = SimpleItem(defvalue = None, typelist=['str','type(None)'], doc='Path prefix to store output files')
    _schema.datadict['path'] = SimpleItem(defvalue = None, typelist=['str','type(None)'], doc='Middle path to store output files')
    _category = 'postprocessor'
    _name = 'ND280Kin_Checker'
    _exportmethods = ['check']

    def check(self,job):
        """
        Checks if .kin file is present and non-zero
        """

        logger.info("Checking/moving outputs of run "+job.name)
        
        self.filesMustExist = False
        self.files = ['*.kin']
        filepaths = self.findFiles(job)
        if len(filepaths) != 1:
            logger.error('Something wrong with kin file(s) '+filepaths+'. CANNOT CONTINUE')
            self.move_output(job,ok=False)
            return False

        kinf = filepaths[0]
        if os.stat(kinf).st_size == 0:
            logger.error('Zero kin file '+kinf+'. CANNOT CONTINUE')
            self.move_output(job,ok=False)
            return False

        logger.info('OK')
        self.move_output(job,ok=True)
        return True    

        
    def move_output(self,job,ok=True):
        dest = os.path.join(self.prfx,self.path)
        jout = 'KIN_'+job.name
            
        task = {'*.kin':'kin','*.root':'kin','*.txt':'aux','*.conf':'aux','stdout':'jobOutput'}
        if not ok:
            task = {'*.kin':'errors','*.root':'errors','*.txt':'errors','*.conf':'errors','stdout':'errors'}
            
        for p in list(task.keys()):
            odir = os.path.join(dest,task[p])

            self.files = [p]
            filepaths = self.findFiles(job)
            for f in filepaths:
                if not os.path.isdir(odir):
                    os.makedirs(odir)

                shutil.move(f,odir)

                # rename stdout in odir
                if p == 'stdout':
                    shutil.move(os.path.join(odir,p),os.path.join(odir,jout))


class ND280RDP_Checker(IFileChecker):
    """
    o Checks .log file in a comprehensive way
    o Posts results to processing DB
    o Moves output files to there destinations
    """
    _schema = IFileChecker._schema.inherit_copy()
    _schema.datadict['prfx'] = SimpleItem(defvalue = None, typelist=['str','type(None)'], doc='Path prefix to store output files')
    _schema.datadict['path'] = SimpleItem(defvalue = None, typelist=['str','type(None)'], doc='Middle path to store output files')
    _schema.datadict['trig'] = SimpleItem(defvalue = None, typelist=['str','type(None)'], doc='Trigger type, e.g. SPILL or COSMIC')
    _schema.datadict['site'] = SimpleItem(defvalue = None, typelist=['str','type(None)'], doc='Processing site, e.g. wg-bugaboo')
    _schema.datadict['post_status'] = SimpleItem(defvalue = None, typelist=['bool','type(None)'], doc='Post job information to processingstatus db')
    _category = 'postprocessor'
    _name = 'ND280RDP_Checker'
    _exportmethods = ['check']
    poststatus = True

    def __init__(self):
        super(ND280RDP_Checker,self).__init__()
        self.filesMustExist = False

        self.InStage = 0
        self.ReturnCode = -1
        self.EventsIn  = 0
        self.EventsOut = 0
        self.Time = 0
        self.STAGE = ''

        self.line = ''

        self.logf = ''
        self.RUN = ''
        self.SUBRUN = ''
        self.TRIGTYPE = None

        self.range = 1 # if to add 0000X000_0000X999 to paths (yes by default)
        
    def find(self,str):
        return self.line.find(str)>=0

    def reset_variables(self):
        self.InStage = 0
        self.ReturnCode = 1
        self.EventsIn  = 0
        self.EventsOut = 0
        self.Time = 0
        self.STAGE = ''

    def send_status(self):
        if not self.post_status:
            return 0

        logger.info('Result for %s %s %s %s is: %s, %s, %s, %s, %s' %  (self.RUN,self.SUBRUN,self.TRIGTYPE,self.STAGE,self.site,self.ReturnCode,self.Time,self.EventsIn,self.EventsOut))

        if self.range == 0: return # no remote status report for CosMC
        
        if not self.path:
            logger.error("No monitoring info sent because MONDIR is not defined")
            return

        rang = self.RUN[:5]+'000_'+self.RUN[:5]+'999'
        mondir = os.path.join(self.path,rang)
        job = { 'run':int(self.RUN), 'subrun':int(self.SUBRUN), 'trigtype':self.TRIGTYPE, 'stage':self.STAGE }
        attributes = {'site':self.site, 'result':self.ReturnCode,
                      'time':int(self.Time), 'read':self.EventsIn, 'written':self.EventsOut }

        try:
            logger.info("%s %s %s",str(mondir), str(job), str(attributes))
            post_status.record(mondir, job, attributes)
        except urllib.error.HTTPError as e:
            logger.error("Error: The HTTP request failed. (%s)" % (str(e), ))
            return 4

        return 0

    def check(self,job):

        IsMC = 0
        IsGenie = 0
        IsNeut = 0
        if self.trig == 'SPILL':
            self.TRIGTYPE = 'spill'
        elif self.trig == 'COSMIC':
            self.TRIGTYPE = 'cosmic'
        elif self.trig == 'MCP' or self.trig == 'MCSAND':
            self.TRIGTYPE = 'spill'
            IsMC = 1
        elif self.trig == 'MCCOS':
            self.TRIGTYPE = 'all'
            IsMC = 1
        else:
            #self.move_outs(job,ok=False)
            #return False
            raise PostProcessException("Unknown type of data: "+self.trig)

        if not self.site:
            #self.move_outs(job,ok=False)
            #return False
            raise PostProcessException('Site is not given')
            
        # finds .log file
        self.files = ['*.log']
        filepaths = self.findFiles(job)
        if len(filepaths) != 1:
            logger.error('Something wrong with logfile(s) '+filepaths+'. CANNOT CONTINUE')
            self.move_output(job,ok=False)
            return False
        self.logf = filepaths[0]

        filename = os.path.basename(self.logf)
        chunks = filename.split('_')
        chunks = chunks[3].split('-')
        self.RUN = chunks[0]
        self.SUBRUN = chunks[1]

        if not os.path.exists(self.logf):
            logger.error("Log file "+filename+" not found. Exit on error")
            self.move_output(job,ok=False)
            return False

        # Check generator if this is a beam MC
        if IsMC == 1:
            if filename.find('oa_nt_')>=0:
                logger.info("This is a NEUT MC log file")
                IsNeut = 1
            elif filename.find('oa_gn_')>=0:
                logger.info("This is a GENIE MC log file")
                IsGenie = 1

        if IsMC == 1:
            self.range = 0 # no range added to paths


        logger.info("Starting to scan file "+filename)
        logger.info("for run %s, subrun %s, type %s" % (self.RUN,self.SUBRUN,self.trig))
        
        inlogf = open(self.logf)
        for self.line in inlogf:
            self.line = self.line.strip('\n')
            if self.find('Midas File') and self.find('has been truncated'):
                logger.error('%s\n%s',self.line,"Midas file probably missing")
                self.ReturnCode = -1
                self.STAGE = "cali"
                self.send_status()
                self.InStage = 0
                break

            elif self.find('Starting job for neutMC.'):
                # neutMC logs are filled by Fluka, they are huge and seems useless
                logger.info(self.line+" The rest of log is ignored.")
                self.ReturnCode = 1
                self.STAGE = "neutMC"
                self.send_status()
                self.InStage = 0
                break
            
            elif self.find('Starting job for nd280MC'):
                self.InStage = 1
                self.STAGE = "nd280MC"
                logger.info(self.line)

            elif self.find('Starting job for elecSim'):
                self.InStage = 1
                self.STAGE = "elecSim"
                logger.info(self.line)

            elif self.find('Starting job for oaCosmicTrigger'):
                self.InStage = 1
                self.STAGE = "COSMICTRIG"
                logger.info(self.line)

            elif self.find('Starting job for oaCalib'):
                self.InStage = 1
                self.STAGE = "cali"
                logger.info(self.line)

            elif self.find('Starting job for oaRecon'):
                self.InStage = 1
                self.STAGE = "reco"
                logger.info(self.line)
 
            elif self.find('Starting job for oaAnalysis'):
                self.InStage = 1
                self.STAGE = "anal"
                logger.info(self.line)

            elif self.find('Starting job for '):
                logger.info(self.line)

            elif self.find('Found Command event_select '):
                logger.info(self.line)
                chunks = self.line.split()
                self.TRIGTYPE = chunks[5]

            elif self.InStage == 1:
                if   self.find('Segmentation fault'):
                    logger.error(self.line)
                    if self.line == '"oaCherryPicker-geo_v5mr.bat: line 7"':
                        logger.warning("This is an acceptable error - ignore it")
                    else:
                        self.ReturnCode = -2
                        self.InStage = 0
                        self.send_status()
                        break

                elif self.find('Disk quota exceeded'):
                    logger.error(self.line)
                    self.ReturnCode = -3
                    self.InStage = 0
                    self.send_status()
                    break

                elif self.find(' ERROR: No database for spillnum'):
                    logger.error(self.line)
                    self.ReturnCode = -8
                    self.InStage = 0
                    self.send_status()
                    break

                elif self.find(' No BSD data available'):
                    logger.error(self.line)
                    self.ReturnCode = -8
                    self.InStage = 0
                    self.send_status()
                    break

                elif self.find('Disabling module '):
                    logger.info("IsMC = "+str(IsMC))
                    if IsMC == 1:
                        logger.info("After testing IsMC = "+str(IsMC))
                        if self.find('Disabling module GRooTrackerVtx') and IsGenie:
                            logger.error("Atest "+self.line)
                            self.ReturnCode = -4
                            self.InStage = 0
                            self.send_status()
                            break
                        elif self.find('Disabling module GRooTrackerVtx') and IsNeut:
                            logger.error("Btest "+self.line)
                            self.ReturnCode = -4
                            self.InStage = 0
                            self.send_status()
                            break
                        elif not self.find('RooTracker'):
                            logger.error("Ctest "+self.line)
                            self.ReturnCode = -4
                            self.InStage = 0
                            self.send_status()
                            break

                elif self.find('probably not closed, trying to recover'):
                    logger.error(self.line)
                    self.ReturnCode = -6
                    self.InStage = 0
                    self.send_status()
                    break

                elif self.find('St9bad_alloc'):
                    logger.error(self.line)
                    self.ReturnCode = -7
                    self.InStage = 0
                    self.send_status()
                    break

                elif self.find('No luck connecting to GSC MySQL server'):
                    logger.error(self.line)
                    self.ReturnCode = -9
                    self.InStage = 0
                    self.send_status()
                    break

                elif self.find('EProductionException'):
                    logger.error(self.line)
                    self.ReturnCode = -11
                    self.InStage = 0
                    self.send_status()
                    break

                elif self.find('Total Events Read'):
                    chunks = self.line.split()
                    self.EventsIn = chunks[3]

                elif self.find('Total Events Written'):
                    chunks = self.line.split()
                    self.EventsOut = chunks[3]

                elif self.find('Number of events ='):
                    chunks = self.line.split()
                    if self.STAGE == 'nd280MC':
                        self.EventsOut = chunks[5]

                elif self.find('Total number of events processed in Analysis'):
                    chunks = self.line.split()
                    self.EventsOut = chunks[9]

                elif self.find('Job Completed Successfully'):
                    logger.info(self.line)
                    nextline = next(inlogf)
                    if nextline.find('Run time')>=0:
                        chunks = nextline.split()
                        Time = chunks[6]
                        chunks = Time.split('.')
                        self.Time = chunks[0]
                    self.ReturnCode = 1
                    if self.STAGE == 'COSMICTRIG':
                        logger.warning("Not sure what this stage is yet - no call to post_status")
                    else:
                        self.send_status()
                    self.reset_variables()
                    self.InStage = 0

        if self.InStage == 1:
            logger.error("The stage "+self.STAGE+" has not completed succesfully, Error is unknown")
            self.ReturnCode = 0
            self.send_status()
            
        inlogf.close()
        logger.info("Finished scanning the log file. Last check return code posted is "+str(self.ReturnCode))

        self.move_output(job)
        return self.ReturnCode == 1
    
    def move_output(self,job,ok=True):
        if ok:
            rang = self.RUN[:5]+'000_'+self.RUN[:5]+'999'
            dest = os.path.join(self.prfx,self.path)
            if self.range:
                dest = os.path.join(self.prfx,self.path,rang)
            jout = self.trig+'_'+self.RUN+'_'+self.SUBRUN
        else:
            dest = os.path.join(self.prfx,self.path)
            jout = 'stdout'
            
        task = {'*_g4mc_*.root':'g4mc','*_elmc_*.root':'elmc','*_cstr_*.root':'cstr',
                '*_numc_*.root':'numc','*_sand_*.root':'sand',
                '*_cali_*.root':'cali','*_reco_*.root':'reco','*_anal_*.root':'anal',
                '*.log':'logf','*catalogue.dat':'cata','stdout':'jobOutput'}
        if self.ReturnCode != 1 or not ok:
            task = {'*.log':'errors','*.root':'errors','*catalogue.dat':'errors','stdout':'errors'}
        for p in list(task.keys()):
            odir = os.path.join(dest,task[p])

            self.files = [p]
            filepaths = self.findFiles(job)
            for f in filepaths:
                if not os.path.isdir(odir):
                    os.makedirs(odir)

                shutil.move(f,odir)
                #os.symlink(f,os.path.join(odir,os.path.basename(f)))

                # rename stdout in odir
                if p == 'stdout':
                    shutil.move(os.path.join(odir,p),os.path.join(odir,jout))

                # "leave" symlink for _cstr_ or _numc_ files in job output directory
                # for possible transform chain
                if task[p] == 'cstr' or task[p] == 'numc':
                    os.symlink(os.path.join(odir,os.path.basename(f)),f)

