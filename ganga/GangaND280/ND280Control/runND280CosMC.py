################################################################################
# GangaND280 Project.
# Dima Vavilov
# Created 21/03/2016
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

class runND280CosMC(IPrepareApp):
    """
    runND280CosMC application running runND280 from nd280Control with configuration files
    (created hereby) specific for Cosmic MC processing
        app = runND280CosMC()

    The required input for this module are:
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
        app.confopts = {'stage':'base', # stage can be 'base','fgd','tript' and 'all'
                        'nd280ver':'v11r31','num_events':'10000000'}

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
    _name = 'runND280CosMC'
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
        super(runND280CosMC,self).__init__()


    def configure(self,masterappconfig):
        
        args = convertIntToStringArgs(self.args)

        job = self.getJobObject()

        if self.cmtsetup == []:
          raise ApplicationConfigurationError('No cmt setup script given.')

        for arg in args:
          if arg == '-c':
            raise ApplicationConfigurationError('Option "-c" given in args. You must use the configfile variable instead.')

        confopts = self.confopts


        # use input file from a "dataset"
        if job.inputdata is None:
            raise ApplicationConfigurationError('The given config file requires an input file but the inputdata of the job is not defined.')
        infiles = job.inputdata.get_dataset_filenames()
        if len(infiles) < 1:
            raise ApplicationConfigurationError('The given config file contains "inputfile" but not input file was given')
        if len(infiles) > 1:
            raise ApplicationConfigurationError('The given config file contains "inputfile" but more than one input file was given')
        confopts.update({'kinfile':infiles[0],'inputfile':infiles[0]})

        # extract "run number" from an input filename
        mtch = re.search(r"(\d{8})",os.path.basename(infiles[0]))
        if mtch:
            job.name = mtch.group(1)
            jobid    = mtch.group(1)
        else:
            raise  ApplicationConfigurationError('Can not extract run number')
        confopts.update({'run_number':str(int(jobid))})

        # create TND280Log config file (in job output dir)
        logConf  = "log.default.level = LogLevel\n"
        logConf += "error.default.level = SevereLevel\n"
        job.getOutputWorkspace().writefile(FileBuffer('nd280log.config',logConf),executable=0)

        # create config file
        cfg = ND280Configs.ND280Config('cosmicmc',confopts)
        inConf = cfg.CreateConfig()
        outConf = inConf
        job.getInputWorkspace().writefile(FileBuffer('nd280.cfg',outConf),executable=0)

        # create a script for a  backend
        args.append('-c')
        args.append(job.inputdir+'nd280.cfg')

        argsStr = ' '.join(args)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        for f in self.cmtsetup:
            script += 'source '+f+'\n'
        script += 'cd '+job.outputdir+'\n'
        script += 'runND280 -t . '+argsStr+'\n'
        #script += 'echo runND280 '+argsStr+' > oa_cs_mu_00003333_' + confopts['stage'] + '.root\n'
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

allHandlers.add('runND280CosMC','LSF', RTHandler)
allHandlers.add('runND280CosMC','Local', RTHandler)
allHandlers.add('runND280CosMC','PBS', RTHandler)
allHandlers.add('runND280CosMC','SGE', RTHandler)
allHandlers.add('runND280CosMC','Slurm', RTHandler)
allHandlers.add('runND280CosMC','Condor', RTHandler)
allHandlers.add('runND280CosMC','LCG', LCGRTHandler)
allHandlers.add('runND280CosMC','gLite', gLiteRTHandler)
allHandlers.add('runND280CosMC','TestSubmitter', RTHandler)
allHandlers.add('runND280CosMC','Interactive', RTHandler)
allHandlers.add('runND280CosMC','Batch', RTHandler)
allHandlers.add('runND280CosMC','Cronus', RTHandler)
allHandlers.add('runND280CosMC','Remote', LCGRTHandler)
allHandlers.add('runND280CosMC','CREAM', LCGRTHandler)

