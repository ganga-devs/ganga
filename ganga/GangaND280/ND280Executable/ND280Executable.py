################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 10/02/2014
################################################################################
"""@package ND280Executable
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

class ND280Executable(IPrepareApp):
    """
    ND280Executable application running any ND280 executables.

    The required input for this module are:
        app.exe = 'RunOARecon.exe'
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
        app.outputfile = 'myFantasticResults.root'

    NOTE: Ganga will run a bash script so the CMT setup script must be in sh/bash.

    It is also possible to pass command line arguments to the executable.
    For example to analyze only 10 events:
        app.args = ['-n',10]
    """
    _schema = Schema(Version(1,1), {
        'exe' : SimpleItem(defvalue=None,typelist=['str', 'type(None)'],comparable=1,doc='A path (string) or a File object specifying an executable.'), 
        'args' : SimpleItem(defvalue=[],typelist=['str','GangaCore.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'cmtsetup' : SimpleItem(defvalue=None,doc='Setup script in bash to set up cmt and the cmt package of the executable.', typelist=['str','type(None)']),
        'outputfile' : SimpleItem(defvalue=None,doc='Output filename or filenames. Takes a string or a list of strings.', typelist=['str','type(None)','list']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        } )
    _category = 'applications'
    _name = 'ND280Executable'
    _scriptname = None
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                  { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'outputfile', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'exe', 'widget' : 'File' },
                          { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'outputfile', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    def __init__(self):
        super(ND280Executable,self).__init__()


    def configure(self,masterappconfig):
        
        self.args = convertIntToStringArgs(self.args)

        job = self.getJobObject()

        if self.cmtsetup is None:
          raise ApplicationConfigurationError('No cmt setup script given.')

        # Need to handle the possibility of multiple output files !
        # setup the output file
        for arg in self.args:
          if arg == '-o':
            raise ApplicationConfigurationError('Option "-o" given in args. You must use the outputfile variable instead, even if you have multiple output files.')

        if self.outputfile is None:
          raise ApplicationConfigurationError('No output file given. Fill the outputfile variable.')
        else:
          if type(self.outputfile) == type([]):
            for OutFi in self.outputfile:
              self.args.append('-o')
              self.args.append(OutFi)
          else:
            self.args.append('-o')
            self.args.append(self.outputfile)


        # So get the list of filenames get_dataset_filenames() and create a file containing the list of files and put it in the sandbox
        if job.inputdata is None:
          raise ApplicationConfigurationError('The inputdata variable is not defined.')
        fileList = job.inputdata.get_dataset_filenames()
        if len(fileList) < 1:
          raise ApplicationConfigurationError('No input data file given.')
        self.args.extend(fileList)

        argsStr = ' '.join(self.args)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        script += 'source '+self.cmtsetup+'\n'
        script += self.exe+' '+argsStr+'\n'

        from GangaCore.GPIDev.Lib.File import FileBuffer

        if self.exe.find('.exe') > -1:
            scriptname = self.exe.replace('.exe', '.sh')
        else:
            scriptname = self.exe + '.sh'
        job.getInputWorkspace().writefile(FileBuffer(scriptname,script),executable=1)

        self._scriptname = job.inputdir+scriptname

        return (None,None)



config = getConfig('defaults_ND280Executable') #_Properties
config.options['exe'].type = type(None)


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

allHandlers.add('ND280Executable','LSF', RTHandler)
allHandlers.add('ND280Executable','Local', RTHandler)
allHandlers.add('ND280Executable','PBS', RTHandler)
allHandlers.add('ND280Executable','SGE', RTHandler)
allHandlers.add('ND280Executable','Slurm', RTHandler)
allHandlers.add('ND280Executable','Condor', RTHandler)
allHandlers.add('ND280Executable','LCG', LCGRTHandler)
allHandlers.add('ND280Executable','gLite', gLiteRTHandler)
allHandlers.add('ND280Executable','TestSubmitter', RTHandler)
allHandlers.add('ND280Executable','Interactive', RTHandler)
allHandlers.add('ND280Executable','Batch', RTHandler)
allHandlers.add('ND280Executable','Cronus', RTHandler)
allHandlers.add('ND280Executable','Remote', LCGRTHandler)
allHandlers.add('ND280Executable','CREAM', LCGRTHandler)

