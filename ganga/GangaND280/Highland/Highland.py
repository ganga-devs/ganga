################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 26/12/2013
################################################################################
"""@package Highland
This module is designed to run any highland executable accessible in the $PATH environment variable.
"""


from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Schema import *

from GangaCore.Utility.Config import getConfig

from GangaCore.GPIDev.Lib.File import *
from GangaCore.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from GangaCore.GPIDev.Base.Proxy import isType
from GangaCore.Core.exceptions import ApplicationConfigurationError

import os, shutil, re
from GangaCore.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

class Highland(IPrepareApp):
    """
    Highland application running any highland executables.

    The required input for this module are:
        app.exe = 'RunNumuCCAnalysis.exe'
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
        app.outputfile = 'myFantasticResults.root'

    NOTE: Ganga will run a bash script so the CMT setup script must be in sh/bash.

    It is also possible to pass command line arguments to the executable.
    For example to analyze only 10 events:
        app.args = ['-n',10]
    
    """
    _schema = Schema(Version(3,3), {
        'exe' : SimpleItem(defvalue=None,typelist=['str', 'type(None)'],comparable=1,doc='A path (string) or a File object specifying an executable.'), 
        'args' : SimpleItem(defvalue=[],typelist=['str','GangaCore.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'cmtsetup' : SimpleItem(defvalue=None,doc='Setup script in bash to set up cmt and the cmt package of the executable.', typelist=['str','type(None)']),
        'outputfile' : SimpleItem(defvalue=None,doc='Output file name.', typelist=['str','type(None)']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        } )
    _category = 'applications'
    _name = 'Highland'
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
        super(Highland,self).__init__()


    def configure(self,masterappconfig):
        
        self.args = convertIntToStringArgs(self.args)

        job = self.getJobObject()

        if self.cmtsetup is None:
          raise ApplicationConfigurationError('No cmt setup script given.')

        # setup the output file
        for arg in self.args:
          if arg == '-o':
            raise ApplicationConfigurationError('Option "-o" given in args. You must use the outputfile variable instead.')

        if self.outputfile is None:
          raise ApplicationConfigurationError('No output file given. Fill the outputfile variable.')
        else:
          self.args.append('-o')
          self.args.append(self.outputfile)


        # So get the list of filenames get_dataset_filenames() and create a file containing the list of files and put it in the sandbox
        fileList = job.inputdir+'FileList'
        if not job.inputdata.set_dataset_into_list(fileList):
          raise ApplicationConfigurationError('Problem with the preparation of the list of input files')
        self.args.append(fileList)

        argsStr = ' '.join(self.args)
        # ANT: Create the bash script here and put it in input dir.
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



config = getConfig('defaults_Highland') #_Properties
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

allHandlers.add('Highland','LSF', RTHandler)
allHandlers.add('Highland','Local', RTHandler)
allHandlers.add('Highland','PBS', RTHandler)
allHandlers.add('Highland','SGE', RTHandler)
allHandlers.add('Highland','Slurm', RTHandler)
allHandlers.add('Highland','Condor', RTHandler)
allHandlers.add('Highland','LCG', LCGRTHandler)
allHandlers.add('Highland','gLite', gLiteRTHandler)
allHandlers.add('Highland','TestSubmitter', RTHandler)
allHandlers.add('Highland','Interactive', RTHandler)
allHandlers.add('Highland','Batch', RTHandler)
allHandlers.add('Highland','Cronus', RTHandler)
allHandlers.add('Highland','Remote', LCGRTHandler)
allHandlers.add('Highland','CREAM', LCGRTHandler)

