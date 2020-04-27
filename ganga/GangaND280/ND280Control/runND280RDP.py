################################################################################
# GangaND280 Project.
# Dima Vavilov
# Created 30/06/2014
################################################################################
"""@package ND280Control
Ganga module to execute runND280 from the nd280Control package.
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

from . import ND280Configs

class runND280RDP(IPrepareApp):
    """
    runND280RDP application running runND280 from nd280Control.
        app = runND280RDP()

    The required input for this module are:
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
        app.confopts = {'nd280ver':'v11r31','event_select':'SPILL'}

    NOTE: Ganga will run a bash script so the CMT setup script must be in sh/bash.

    It is also possible to pass command line arguments to the executable.
    For example to use '/home/me/tmp' as temporary directory:
        app.args = ['-t','/home/me/tmp']

    """
    _schema = Schema(Version(1,1), {
        'args' : SimpleItem(defvalue=[],typelist=['str','GangaCore.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'cmtsetup' : SimpleItem(defvalue=[],doc='Setup script(s) in bash to set up cmt and the cmt package of the executable.', typelist=['str'],sequence=1,strict_sequence=0),
        'confopts' : SimpleItem(defvalue={},doc='Options for configuration file', typelist=['str']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        } )
    _category = 'applications'
    _name = 'runND280RDP'
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                  { 'attribute' : 'confopts', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                          { 'attribute' : 'confopts', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]


    def __init__(self):
        super(runND280RDP,self).__init__()

    def configure(self,masterappconfig):

        args = convertIntToStringArgs(self.args)

        job = self.getJobObject()

        if self.cmtsetup == []:
          raise ApplicationConfigurationError('No cmt setup script given.')

        for arg in args:
          if arg == '-c':
            raise ApplicationConfigurationError('Option "-c" given in args. You must use the configfile variable instead.')


        cfg = ND280Configs.ND280Config('raw',self.confopts)
        inConf = cfg.CreateConfig()
        outConf = ''

        for line in inConf.splitlines(True):
            inputfile_listfnd = re.match(r"^inputfile_list\s*=", line)
            inputfilefnd = re.match(r"^inputfile\s*=", line)
            midas_filefnd = re.match(r"^midas_file\s*=", line)
            if inputfile_listfnd or inputfilefnd or midas_filefnd:
              if job.inputdata is None:
                raise ApplicationConfigurationError('The given config file requires an input file but the inputdata of the job is not defined.')
              # TODO: Check if there is an inputdata
              infiles = job.inputdata.get_dataset_filenames()
              if len(infiles) < 1:
                raise ApplicationConfigurationError('The given config file contains "inputfile" but not input file was given')
              if inputfile_listfnd:
                line = 'inputfile_list = ' + ' '.join(infiles) + '\n'
              elif inputfilefnd:
                if len(infiles) > 1:
                  raise ApplicationConfigurationError('The given config file contains "inputfile" but more than one input file was given')
                line = 'inputfile = ' + infiles[0] + '\n'
              elif midas_filefnd:
                if len(infiles) > 1:
                  raise ApplicationConfigurationError('The given config file contains "midas_file" but more than one file was given')
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
        script += 'cd '+job.outputdir+'\n'
        script += 'runND280 '+argsStr+'\n'
        job.getInputWorkspace().writefile(FileBuffer('runND280.sh',script),executable=1)

        self._scriptname = job.inputdir+'runND280.sh'

        # Possibly gives job a name after run/subrun numbers
        if job.inputdata:
            infiles = job.inputdata.get_dataset_filenames()
            mtch = re.search(r"(\d{8})[_-](\d{4})",os.path.basename(infiles[0]))
            if mtch:
                job.name = str(int(mtch.group(1)))+"_"+mtch.group(2)

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
        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        c = StandardJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)
        return c


class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.Lib.LCG import LCGJobConfig

        return LCGJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

class gLiteRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaCore.Lib.gLite import gLiteJobConfig

        return gLiteJobConfig(app._scriptname,app._getParent().inputsandbox,[],app._getParent().outputsandbox,app.env)

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('runND280RDP','LSF', RTHandler)
allHandlers.add('runND280RDP','Local', RTHandler)
allHandlers.add('runND280RDP','PBS', RTHandler)
allHandlers.add('runND280RDP','SGE', RTHandler)
allHandlers.add('runND280RDP','Slurm', RTHandler)
allHandlers.add('runND280RDP','Condor', RTHandler)
allHandlers.add('runND280RDP','LCG', LCGRTHandler)
allHandlers.add('runND280RDP','gLite', gLiteRTHandler)
allHandlers.add('runND280RDP','TestSubmitter', RTHandler)
allHandlers.add('runND280RDP','Interactive', RTHandler)
allHandlers.add('runND280RDP','Batch', RTHandler)
allHandlers.add('runND280RDP','Cronus', RTHandler)
allHandlers.add('runND280RDP','Remote', LCGRTHandler)
allHandlers.add('runND280RDP','CREAM', LCGRTHandler)
allHandlers.add('runND280RDP','Batch', RTHandler)
