################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created  5/02/2015
################################################################################
"""@package TRExPlusOAAnalysis
This module is designed to run TREx followed by oaAnalysis
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

class TRExPlusOAAnalysis(IPrepareApp):
    """
    TRExPlusOAAnalysis application running any ND280 executables.

    The required input for this module are:
        app.filenamesubstr = 'myFantasticVFT.root'
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'

    NOTE: Ganga will run a bash script so the CMT setup script must be in sh/bash.

    It is also possible to pass command line arguments to the executable.
    For example to analyze only 10 events:
        app.trex_args = ['-n',10]
    """
    _schema = Schema(Version(1,1), {
        'cmtsetup' : SimpleItem(defvalue=None,doc='Setup script in bash to set up cmt and the cmt package of the executable.', typelist=['str','type(None)']),
        'trex_args' : SimpleItem(defvalue=[],typelist=['str','GangaCore.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'oaana_only' : SimpleItem(defvalue=False,doc='Run only oaAnalysis on the input files, i.e. skip the reconstruction.', typelist=['bool']),
        'oaana_args' : SimpleItem(defvalue=[],typelist=['str','GangaCore.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'filenamesubstr' : SimpleItem(defvalue=None,doc='This string will be substituted by "trex" in the TREx output filename and "anal" in the oaAnalysis output filename.', typelist=['str','type(None)']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        } )
    _category = 'applications'
    _name = 'TRExPlusOAAnalysis'
    _scriptname = None
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'trex_args', 'widget' : 'String_List' },
                  { 'attribute' : 'oaana_args', 'widget' : 'String_List' },
                  { 'attribute' : 'filenamesubstr', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'trex_args', 'widget' : 'String_List' },
                          { 'attribute' : 'oaana_args', 'widget' : 'String_List' },
                          { 'attribute' : 'filenamesubstr', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    def __init__(self):
        super(TRExPlusOAAnalysis,self).__init__()


    def configure(self,masterappconfig):
        if self.cmtsetup is None:
          raise ApplicationConfigurationError('No cmt setup script given.')

        # __________ TREx first ____________
        trex_args = convertIntToStringArgs(self.trex_args)

        job = self.getJobObject()

        # Need to handle the possibility of multiple output files !
        # setup the output file
        for arg in trex_args:
          if arg == '-o':
            raise ApplicationConfigurationError('Option "-o" given in trex_args. The module will define the output filename.')

        # So get the list of filenames get_dataset_filenames() and create a file containing the list of files and put it in the sandbox
        if job.inputdata is None:
          raise ApplicationConfigurationError('The inputdata variable is not defined.')
        fileList = job.inputdata.get_dataset_filenames()
        if len(fileList) < 1:
          raise ApplicationConfigurationError('No input data file given.')
        trex_args.extend(fileList)

        firstFile = fileList[0].split('/')[-1]
        # Define the output
        trex_args.append('-o')
        if self.filenamesubstr is None:
          trex_outputfile = 'recoOutput.root'
        else:
          trex_outputfile = firstFile.replace(self.filenamesubstr, "trex")

        trex_args.append(trex_outputfile)

        # __________ Now oaAnalysis ____________
        oaana_args = convertIntToStringArgs(self.oaana_args)

        job = self.getJobObject()

        # Need to handle the possibility of multiple output files !
        # setup the output file
        for arg in oaana_args:
          if arg == '-o':
            raise ApplicationConfigurationError('Option "-o" given in oaana_args. You must use the oaana_outputfile variable instead.')

        oaana_args.append('-o')
        if self.filenamesubstr is None:
          oaana_outputfile = 'recoOutput.root'
        else:
          oaana_outputfile = firstFile.replace(self.filenamesubstr, "anal")
          # protection against failed substitution
          if oaana_outputfile == trex_outputfile:
            oaana_outputfile = oaana_outputfile.replace(".root","_anal.root")
        oaana_args.append(oaana_outputfile)

        # Use the reco output as an input for the VFT processing.
        if self.oaana_only:
            oaana_args.extend(fileList)
        else:
            oaana_args.append(trex_outputfile)

        trex_argsStr = ' '.join(trex_args)
        oaana_argsStr = ' '.join(oaana_args)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        script += 'source '+self.cmtsetup+'\n'
        if not self.oaana_only:
            script += 'RunTREx.exe '+trex_argsStr+'\n'
        script += 'RunOAAnalysis.exe '+oaana_argsStr+'\n'

        from GangaCore.GPIDev.Lib.File import FileBuffer

        scriptname = 'TRExPlusOAAnalysis.sh'
        job.getInputWorkspace().writefile(FileBuffer(scriptname,script),executable=1)

        self._scriptname = job.inputdir+scriptname

        return (None,None)



config = getConfig('defaults_TRExPlusOAAnalysis') #_Properties


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

allHandlers.add('TRExPlusOAAnalysis','LSF', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','Local', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','PBS', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','SGE', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','Slurm', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','Condor', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','LCG', LCGRTHandler)
allHandlers.add('TRExPlusOAAnalysis','gLite', gLiteRTHandler)
allHandlers.add('TRExPlusOAAnalysis','TestSubmitter', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','Interactive', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','Batch', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','Cronus', RTHandler)
allHandlers.add('TRExPlusOAAnalysis','Remote', LCGRTHandler)
allHandlers.add('TRExPlusOAAnalysis','CREAM', LCGRTHandler)

