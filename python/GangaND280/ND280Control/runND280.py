################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 17/01/2014
################################################################################
"""@package ND280Control
Ganga module to execute runND280 from the nd280Control package.
"""

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.Core import ApplicationConfigurationError

import os, shutil, commands, re
from Ganga.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

class runND280(IApplication):
    """
    runND280 application running runND280 from nd280Control.
        app = runND280()

    The required input for this module are:
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
        app.configfile = 'mynd280config.cfg'

    NOTE: Ganga will run a bash script so the CMT setup script must be in sh/bash.

    It is also possible to pass command line arguments to the executable.
    For example to use '/home/me/tmp' as temporary directory:
        app.args = ['-t','/home/me/tmp']

    """
    _schema = Schema(Version(1,1), {
        'args' : SimpleItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'cmtsetup' : SimpleItem(defvalue=[],doc='Setup script(s) in bash to set up cmt and the cmt package of the executable.', typelist=['str'],sequence=1,strict_sequence=0),
        'configfile' : SimpleItem(defvalue=None,doc='Filename of the nd280Control config file.', typelist=['str','type(None)']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        } )
    _category = 'applications'
    _name = 'runND280'
    _exportmethods = []
    _GUIPrefs = [ { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                  { 'attribute' : 'configfile', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                          { 'attribute' : 'configfile', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]
    exe = 'runND280'

    def __init__(self):
        super(runND280,self).__init__()


    def configure(self,masterappconfig):
        
        args = convertIntToStringArgs(self.args)

        job = self.getJobObject()

        if self.cmtsetup == []:
          raise ApplicationConfigurationError(None,'No cmt setup script given.')

        for arg in args:
          if arg == '-c':
            raise ApplicationConfigurationError(None,'Option "-c" given in args. You must use the configfile variable instead.')

        # setup the config file for this job
        if self.configfile == None:
          raise ApplicationConfigurationError(None,'No config file given. Use args list or configfile field.')
        # check if given config file exists
        if not os.path.exists(self.configfile):
          raise ApplicationConfigurationError(None,'The given config file "'+self.configfile+'" was not found.')
        if not os.path.isfile(self.configfile):
          raise ApplicationConfigurationError(None,'The given config file "'+self.configfile+'" is not a file.')

        # Right here, take the input config file and change it as needed
        # If found inputfile, just put the first file in the inputdata
        # If this is inputfile_list, then it is in cherry pick so we can put all the files from inputdata
        inConf = open(self.configfile)
        outConf = ''

        for line in inConf:
            inputfile_listfnd = re.match(r"^inputfile_list\s*=", line)
            inputfilefnd = re.match(r"^inputfile\s*=", line)
            midas_filefnd = re.match(r"^midas_file\s*=", line)
            if inputfile_listfnd or inputfilefnd or midas_filefnd:
              if job.inputdata == None:
                raise ApplicationConfigurationError(None,'The given config file requires an input file but the inputdata of the job is not defined.')
              # TODO: Check if there is an inputdata
              infiles = job.inputdata.get_dataset_filenames()
              if len(infiles) < 1:
                raise ApplicationConfigurationError(None,'The given config file contains "inputfile" but not input file was given')
              if inputfile_listfnd:
                line = 'inputfile_list = ' + ' '.join(infiles) + '\n'
              elif inputfilefnd:
                if len(infiles) > 1:
                  raise ApplicationConfigurationError(None,'The given config file contains "inputfile" but more than one input file was given')
                line = 'inputfile = ' + infiles[0] + '\n'
              elif midas_filefnd:
                if len(infiles) > 1:
                  raise ApplicationConfigurationError(None,'The given config file contains "midas_file" but more than one file was given')
                line = 'midas_file = ' + infiles[0] + '\n'
            
            outConf += line
        job.getInputWorkspace().writefile(FileBuffer('nd280Config.cfg',outConf),executable=0)

        args.append('-c')
        args.append(job.inputdir+'nd280Config.cfg')

        argsStr = ' '.join(args)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        for f in self.cmtsetup:
            script += 'source '+f+'\n'
        script += 'runND280 '+argsStr+'\n'
        job.getInputWorkspace().writefile(FileBuffer('runND280.sh',script),executable=1)

        self._scriptname = job.inputdir+'runND280.sh'

        return (None,None)



config = getConfig('defaults_runND280') #_Properties
# config.options['exe'].type = type(None)


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

        c = StandardJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)
        return c
        

class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.LCG import LCGJobConfig

        return LCGJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.gLite import gLiteJobConfig

        return gLiteJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('runND280','LSF', RTHandler)
allHandlers.add('runND280','Local', RTHandler)
allHandlers.add('runND280','PBS', RTHandler)
allHandlers.add('runND280','SGE', RTHandler)
allHandlers.add('runND280','Condor', RTHandler)
allHandlers.add('runND280','LCG', LCGRTHandler)
allHandlers.add('runND280','gLite', gLiteRTHandler)
allHandlers.add('runND280','TestSubmitter', RTHandler)
allHandlers.add('runND280','Interactive', RTHandler)
allHandlers.add('runND280','Batch', RTHandler)
allHandlers.add('runND280','Cronus', RTHandler)
allHandlers.add('runND280','Remote', LCGRTHandler)
allHandlers.add('runND280','CREAM', LCGRTHandler)

