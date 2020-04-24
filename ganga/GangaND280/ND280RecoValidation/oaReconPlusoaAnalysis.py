################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 19/02/2015
################################################################################
"""@package oaReconPlusoaAnalysis
This module is designed to run any ND280 executable accessible in the $PATH environment variable.
"""

from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import *

from GangaCore.Utility.Config import getConfig

from GangaCore.GPIDev.Lib.File import *
from GangaCore.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.Core.exceptions import ApplicationConfigurationError

import os, shutil, re, time
from GangaCore.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

class oaReconPlusoaAnalysis(IPrepareApp):
    """
    oaReconPlusoaAnalysis application running any ND280 executables.

    The required input for this module are:
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
    You can modify the output filename by replacing a string of the input filename.
    For example to replace "reco" in the input filename by "newreco" in the output of oaRecon:
        app.filenamesubstr = "reco"
        app.reconewstr = "newreco"


    NOTE: Ganga will run a bash script so the CMT setup script must be in sh/bash.

    It is also possible to pass command line arguments to the executable.
    For example to analyze only 10 events:
        app.reco_args = ['-n',10]
    Or to use a customized parameters file:
        app.reco_args = ['-O','par_override=/home/me/myparametersfile.dat']
    """
    _schema = Schema(Version(1,1), {
        'cmtsetup' : SimpleItem(defvalue=None,doc='Setup script in bash to set up cmt and the cmt package of the executable.', typelist=['str','type(None)']),
        'reco_args' : SimpleItem(defvalue=[],typelist=['str','GangaCore.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'anal_args' : SimpleItem(defvalue=[],typelist=['str','GangaCore.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'filenamesubstr' : SimpleItem(defvalue=None,doc='This string will be substituted by recnewstr variable in the reco output filename and analnewstr variable in the oaAnalysis output filename.', typelist=['str','type(None)']),
        'reconewstr' : SimpleItem(defvalue='newreco',doc='This string will substitute filenamesubstr in the input filename to create the reco output filename.', typelist=['str']),
        'analnewstr' : SimpleItem(defvalue='validtree',doc='This string will substitute filenamesubstr in the input filename to create the oaAnalysis output filename.', typelist=['str']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        } )
    _category = 'applications'
    _name = 'oaReconPlusoaAnalysis'
    _scriptname = None
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'reco_args', 'widget' : 'String_List' },
                  { 'attribute' : 'anal_args', 'widget' : 'String_List' },
                  { 'attribute' : 'filenamesubstr', 'widget' : 'String' },
                  { 'attribute' : 'reconewstr', 'widget' : 'String' },
                  { 'attribute' : 'analnewstr', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'reco_args', 'widget' : 'String_List' },
                          { 'attribute' : 'anal_args', 'widget' : 'String_List' },
                          { 'attribute' : 'filenamesubstr', 'widget' : 'String' },
                          { 'attribute' : 'reconewstr', 'widget' : 'String' },
                          { 'attribute' : 'analnewstr', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    def __init__(self):
        super(oaReconPlusoaAnalysis,self).__init__()


    def configure(self,masterappconfig):
        if self.cmtsetup is None:
          raise ApplicationConfigurationError('No cmt setup script given.')

        # __________ Reco first ____________
        reco_args = convertIntToStringArgs(self.reco_args)

        job = self.getJobObject()

        # Need to handle the possibility of multiple output files !
        # setup the output file
        for arg in reco_args:
          if arg == '-o':
            raise ApplicationConfigurationError('Option "-o" given in reco_args. You must use the filenamesubstr and reconewstr variables instead to define an output.')

        # So get the list of filenames get_dataset_filenames() and create a file containing the list of files and put it in the sandbox
        if job.inputdata is None:
          raise ApplicationConfigurationError('The inputdata variable is not defined.')
        fileList = job.inputdata.get_dataset_filenames()
        if len(fileList) < 1:
          raise ApplicationConfigurationError('No input data file given.')

        firstFile = fileList[0].split('/')[-1]
        # Define the output
        reco_args.append('-o')
        if self.filenamesubstr is None:
          reco_outputfile = 'recoOutput.root'
        else:
          reco_outputfile = firstFile.replace(self.filenamesubstr, self.reconewstr)

        reco_args.append(reco_outputfile)
        
        # Just to define the output before the potentially long list of input files
        reco_args.extend(fileList)

        # __________ Now oaAnalysis ____________
        anal_args = convertIntToStringArgs(self.anal_args)

        job = self.getJobObject()

        # Need to handle the possibility of multiple output files !
        # setup the output file
        for arg in anal_args:
          if arg == '-o':
            raise ApplicationConfigurationError('Option "-o" given in anal_args. You must use the filenamesubstr and reconewstr variables instead to define an output.')

        # Define the output
        anal_args.append('-o')
        if self.filenamesubstr is None:
          anal_outputfile = 'analOutput.root'
        else:
          anal_outputfile = firstFile.replace(self.filenamesubstr, self.analnewstr)
        anal_args.append(anal_outputfile)

        # Now add the input file
        anal_args.append(reco_outputfile)

        reco_argsStr = ' '.join(reco_args)
        anal_argsStr = ' '.join(anal_args)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        script += 'source '+self.cmtsetup+'\n'
        script += 'RunOARecon.exe '+reco_argsStr+'\n'
        script += 'RunOAAnalysis.exe '+anal_argsStr+'\n'

        from GangaCore.GPIDev.Lib.File import FileBuffer

        scriptname = 'oaReconPlusoaAnalysis.sh'
        job.getInputWorkspace().writefile(FileBuffer(scriptname,script),executable=1)

        self._scriptname = job.inputdir+scriptname

        return (None,None)



config = getConfig('defaults_oaReconPlusoaAnalysis') #_Properties


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

allHandlers.add('oaReconPlusoaAnalysis','LSF', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','Local', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','PBS', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','SGE', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','Slurm', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','Condor', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','LCG', LCGRTHandler)
allHandlers.add('oaReconPlusoaAnalysis','gLite', gLiteRTHandler)
allHandlers.add('oaReconPlusoaAnalysis','TestSubmitter', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','Interactive', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','Batch', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','Cronus', RTHandler)
allHandlers.add('oaReconPlusoaAnalysis','Remote', LCGRTHandler)
allHandlers.add('oaReconPlusoaAnalysis','CREAM', LCGRTHandler)

