################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 23/04/2014
################################################################################
"""@package ND280Skimmer
Ganga module with classes to skim from reco files a set of events listed in a CSV file.
"""

from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import *

from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.logging import getLogger

from GangaCore.GPIDev.Lib.File import *
from GangaCore.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.Core.exceptions import ApplicationConfigurationError

logger = getLogger()

from os.path import isfile

import os, shutil, re, time
from GangaCore.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

class ND280RecoSkimmer(IPrepareApp):
    """
    ND280RecoSkimmer application to skim reco files from a list of run, subrun and event numbers in a CSV file.

    The required input for this module are:
        app.csvfile = '/home/me/myCSVfile.txt'
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
        app.outputfile = 'myFantasticResults.root'
    
    """
    _schema = Schema(Version(1,1), {
        'csvfile' : SimpleItem(defvalue=None,doc='CSV file containing "run,subrun,event" numbers.', typelist=['str','type(None)']),
        'cmtsetup' : SimpleItem(defvalue=None,doc='Setup script in bash to set up cmt and the cmt package of the executable.', typelist=['str','type(None)']),
        'outputfile' : SimpleItem(defvalue=None,doc='Output filename or filenames. Takes a string or a list of strings.', typelist=['str','type(None)','list']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        } )
    _category = 'applications'
    _name = 'ND280RecoSkimmer'
    _scriptname = None
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'csvfile', 'widget' : 'String' },
                  { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                  { 'attribute' : 'outputfile', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'csvfile', 'widget' : 'String' },
                          { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                          { 'attribute' : 'outputfile', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    def __init__(self):
        super(ND280RecoSkimmer,self).__init__()


    def configure(self,masterappconfig):
        
        exefile = 'skimFromCSV.exe'
        exe = 'skimFromCSV.exe'
        # exe = '/'.join([os.getenv("RECONUTILSROOT"),os.getenv("CMTCONFIG"),exefile])
        # if not isfile(exe):
        #   raise ApplicationConfigurationError(None,'Cannot find executable '+exe)

        job = self.getJobObject()

        if self.cmtsetup is None:
          raise ApplicationConfigurationError('No cmt setup script given.')
        if not isfile(self.cmtsetup):
          raise ApplicationConfigurationError('Cannot find cmt setup script '+self.cmtsetup)

        # Copy CSV file to inputdir. Done in splitter for subjobs.
        if not isfile(self.csvfile):
          raise ApplicationConfigurationError('Cannot find CSV file '+self.csvfile)
        from shutil import copy
        tmpcsv = os.path.join(job.inputdir, os.path.basename(self.csvfile))
        if not os.path.exists(tmpcsv):
          copy(self.csvfile, job.inputdir)
        self.csvfile = tmpcsv

        args = []

        args.append('-O')
        args.append('file='+self.csvfile)

        if self.outputfile is None:
          raise ApplicationConfigurationError('No output file given. Fill the outputfile variable.')

        args.append('-o')
        args.append(self.outputfile)

        # Read the CSV file
        csvfile = open(self.csvfile, 'rb')
        run_subrun = []
        for line in csvfile:
            if line[0] == '#':
              continue
            row = line.split(",")
            if len(row) < 3:
              logger.info("Ignoring badly-formatted line:", ",".join(row))
              continue

            r_sr = "%(run)08d-%(subrun)04d" % { "run" : int(row[0]), "subrun" : int(row[1]) }
            if r_sr not in run_subrun:
              run_subrun.append(r_sr)

        # So get the list of filenames get_dataset_filenames() and create a file containing the list of files and put it in the sandbox
        if job.inputdata is None:
          raise ApplicationConfigurationError('The inputdata variable is not defined.')
        rawFileList = job.inputdata.get_dataset_filenames()
        if len(rawFileList) < 1:
          raise ApplicationConfigurationError('No input data file given.')

        fileList = []
        for r_sr in run_subrun:
          for rfile in rawFileList:
            if rfile.find(r_sr) > -1:
              fileList.append(rfile)
              continue
        if not len(fileList):
          raise ApplicationConfigurationError('No file matching the run_subrun in the CSV file %s.' % self.csvfile)
        args.extend(fileList)

        argsStr = ' '.join(args)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        script += 'source '+self.cmtsetup+'\n'
        script += '${RECONUTILSROOT}/${CMTCONFIG}/'+exe+' '+argsStr+'\n'
        # Little trick to be able to control the final destination
        # of the subjob's CSV file with SandboxFile or SharedFile
        if job.master is not None:
            script += 'cp %s .' % self.csvfile

        from GangaCore.GPIDev.Lib.File import FileBuffer

        if exefile.find('.exe') > -1:
            scriptname = exefile.replace('.exe', '.sh')
        else:
            scriptname = exefile + '.sh'
        job.getInputWorkspace().writefile(FileBuffer(scriptname,script),executable=1)

        self._scriptname = job.inputdir+scriptname

        return (None,None)



config = getConfig('defaults_ND280RecoSkimmer') #_Properties


def convertIntToStringArgs(args):

    result = []
    
    for arg in args:
        if isinstance(arg,int):
            result.append(str(arg))
        else:
            result.append(arg)

    return result



class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        return StandardJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)
        

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.Lib.LCG import LCGJobConfig

        return LCGJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.Lib.gLite import gLiteJobConfig

        return gLiteJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('ND280RecoSkimmer','LSF', RTHandler)
allHandlers.add('ND280RecoSkimmer','Local', RTHandler)
allHandlers.add('ND280RecoSkimmer','PBS', RTHandler)
allHandlers.add('ND280RecoSkimmer','SGE', RTHandler)
allHandlers.add('ND280RecoSkimmer','Slurm', RTHandler)
allHandlers.add('ND280RecoSkimmer','Condor', RTHandler)
allHandlers.add('ND280RecoSkimmer','LCG', LCGRTHandler)
allHandlers.add('ND280RecoSkimmer','gLite', gLiteRTHandler)
allHandlers.add('ND280RecoSkimmer','TestSubmitter', RTHandler)
allHandlers.add('ND280RecoSkimmer','Interactive', RTHandler)
allHandlers.add('ND280RecoSkimmer','Batch', RTHandler)
allHandlers.add('ND280RecoSkimmer','Cronus', RTHandler)
allHandlers.add('ND280RecoSkimmer','Remote', LCGRTHandler)
allHandlers.add('ND280RecoSkimmer','CREAM', LCGRTHandler)

