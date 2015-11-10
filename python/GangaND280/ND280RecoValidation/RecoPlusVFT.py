################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 10/02/2014
################################################################################
"""@package RecoPlusVFT
This module is designed to run any ND280 executable accessible in the $PATH environment variable.
"""

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IPrepareApp import IPrepareApp
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.Core import ApplicationConfigurationError

import os, shutil, commands, re, time
from Ganga.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

class RecoPlusVFT(IApplication):
    """
    RecoPlusVFT application running any ND280 executables.

    The required input for this module are:
        app.reco_exe = 'RunOARecon.exe'
        app.vft_exe = 'OAReconValidTree.exe'
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'

    NOTE: Ganga will run a bash script so the CMT setup script must be in sh/bash.

    It is also possible to pass command line arguments to the executable.
    For example to analyze only 10 events:
        app.reco_args = ['-n',10]
    """
    _schema = Schema(Version(1,1), {
        'cmtsetup' : SimpleItem(defvalue=None,doc='Setup script in bash to set up cmt and the cmt package of the executable.', typelist=['str','type(None)']),
        'reco_exe' : SimpleItem(defvalue=None,typelist=['str', 'type(None)'],comparable=1,doc='A path (string) or a File object specifying the reconstruction executable. For example oaRecon.exe.'), 
        'reco_args' : SimpleItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'vft_only' : SimpleItem(defvalue=True,doc='Run only VFT on the input files, i.e. don\'t call a reconstruction software.', typelist=['bool']),
        'vft_exe' : SimpleItem(defvalue=None,typelist=['str', 'type(None)'],comparable=1,doc='A path (string) or a File object specifying the validation flat tree executable. For example OAReconValidTree.exe.'), 
        'vft_args' : SimpleItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'filenamesubstr' : SimpleItem(defvalue=None,doc='This string will be substituted by recnewstr variable in the reco output filename and vftnewstr variable in the VFT output filename.', typelist=['str','type(None)']),
        'reconewstr' : SimpleItem(defvalue='newreco',doc='This string will substitute filenamesubstr in the input filename to create the reco output filename.', typelist=['str']),
        'vftnewstr' : SimpleItem(defvalue='validtree',doc='This string will substitute filenamesubstr in the input filename to create the VFT output filename.', typelist=['str']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        } )
    _category = 'applications'
    _name = 'RecoPlusVFT'
    _scriptname = None
    _exportmethods = []
    _GUIPrefs = [ { 'attribute' : 'reco_exe', 'widget' : 'File' },
                  { 'attribute' : 'reco_args', 'widget' : 'String_List' },
                  { 'attribute' : 'vft_exe', 'widget' : 'File' },
                  { 'attribute' : 'vft_args', 'widget' : 'String_List' },
                  { 'attribute' : 'filenamesubstr', 'widget' : 'String' },
                  { 'attribute' : 'reconewstr', 'widget' : 'String' },
                  { 'attribute' : 'vftnewstr', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'reco_exe', 'widget' : 'File' },
                          { 'attribute' : 'reco_args', 'widget' : 'String_List' },
                          { 'attribute' : 'vft_exe', 'widget' : 'File' },
                          { 'attribute' : 'vft_args', 'widget' : 'String_List' },
                          { 'attribute' : 'filenamesubstr', 'widget' : 'String' },
                          { 'attribute' : 'reconewstr', 'widget' : 'String' },
                          { 'attribute' : 'vftnewstr', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    def __init__(self):
        super(RecoPlusVFT,self).__init__()


    def configure(self,masterappconfig):
        if self.cmtsetup == None:
          raise ApplicationConfigurationError(None,'No cmt setup script given.')

        # __________ Reco first ____________
        reco_args = convertIntToStringArgs(self.reco_args)

        job = self.getJobObject()

        # Need to handle the possibility of multiple output files !
        # setup the output file
        for arg in reco_args:
          if arg == '-o':
            raise ApplicationConfigurationError(None,'Option "-o" given in reco_args. You must use the filenamesubstr and reconewstr variables instead to define an output.')

        # So get the list of filenames get_dataset_filenames() and create a file containing the list of files and put it in the sandbox
        if job.inputdata == None:
          raise ApplicationConfigurationError(None,'The inputdata variable is not defined.')
        fileList = job.inputdata.get_dataset_filenames()
        if len(fileList) < 1:
          raise ApplicationConfigurationError(None,'No input data file given.')

        firstFile = fileList[0].split('/')[-1]
        # Define the output
        reco_args.append('-o')
        if self.filenamesubstr == None:
          reco_outputfile = 'recoOutput.root'
        else:
          reco_outputfile = firstFile.replace(self.filenamesubstr, self.reconewstr)

        reco_args.append(reco_outputfile)
        
        # Just to define the output before the potentially long list of input files
        reco_args.extend(fileList)

        # __________ Now VFT ____________
        vft_args = convertIntToStringArgs(self.vft_args)

        job = self.getJobObject()

        # Need to handle the possibility of multiple output files !
        # setup the output file
        for arg in vft_args:
          if arg == '-o':
            raise ApplicationConfigurationError(None,'Option "-o" given in vft_args. You must use the filenamesubstr and reconewstr variables instead to define an output.')

        # Define the output
        vft_args.append('-o')
        if self.filenamesubstr == None:
          vft_outputfile = 'vftOutput.root'
        else:
          vft_outputfile = firstFile.replace(self.filenamesubstr, self.vftnewstr)
        vft_args.append(vft_outputfile)

        # Use the reco output as an input for the VFT processing
        # or use the input file list if running in VFT only mode.
        if self.vft_only:
            vft_args.extend(fileList)
        else:
            vft_args.append(reco_outputfile)

        reco_argsStr = ' '.join(reco_args)
        vft_argsStr = ' '.join(vft_args)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        script += 'source '+self.cmtsetup+'\n'
        if not self.vft_only:
            script += self.reco_exe+' '+reco_argsStr+'\n'
        script += self.vft_exe+' '+vft_argsStr+'\n'

        from Ganga.GPIDev.Lib.File import FileBuffer

        scriptname = 'RecoPlusVFT.sh'
        job.getInputWorkspace().writefile(FileBuffer(scriptname,script),executable=1)

        self._scriptname = job.inputdir+scriptname

        return (None,None)



config = getConfig('defaults_RecoPlusVFT') #_Properties
config.options['reco_exe'].type = type(None)
config.options['vft_exe'].type = type(None)


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
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        return StandardJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)
        

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.LCG import LCGJobConfig

        return LCGJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.gLite import gLiteJobConfig

        return gLiteJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('RecoPlusVFT','LSF', RTHandler)
allHandlers.add('RecoPlusVFT','Local', RTHandler)
allHandlers.add('RecoPlusVFT','PBS', RTHandler)
allHandlers.add('RecoPlusVFT','SGE', RTHandler)
allHandlers.add('RecoPlusVFT','Condor', RTHandler)
allHandlers.add('RecoPlusVFT','LCG', LCGRTHandler)
allHandlers.add('RecoPlusVFT','gLite', gLiteRTHandler)
allHandlers.add('RecoPlusVFT','TestSubmitter', RTHandler)
allHandlers.add('RecoPlusVFT','Interactive', RTHandler)
allHandlers.add('RecoPlusVFT','Batch', RTHandler)
allHandlers.add('RecoPlusVFT','Cronus', RTHandler)
allHandlers.add('RecoPlusVFT','Remote', LCGRTHandler)
allHandlers.add('RecoPlusVFT','CREAM', LCGRTHandler)

