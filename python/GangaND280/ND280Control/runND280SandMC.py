################################################################################
# GangaND280 Project.
# Dima Vavilov
# Created 21/03/2016
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

import ND280Configs

class runND280SandMC(IApplication):
    """
    runND280SandMC application running runND280 from nd280Control with configuration files
    (created hereby) specific for Sand MC processing
        app = runND280SandMC()

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
        'args' : SimpleItem(defvalue=[],typelist=['str','Ganga.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'cmtsetup' : SimpleItem(defvalue=[],doc='Setup script(s) in bash to set up cmt and the cmt package of the executable.', typelist=['str'],sequence=1,strict_sequence=0),
        'confopts' : SimpleItem(defvalue={},doc='Options for configuration file', typelist=['str']),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        } )
    _category = 'applications'
    _name = 'runND280SandMC'
    _exportmethods = []
    _GUIPrefs = [ { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                  { 'attribute' : 'confopts', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                          { 'attribute' : 'confopts', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]


    def __init__(self):
        super(runND280SandMC,self).__init__()


    def configure(self,masterappconfig):
        
        args = convertIntToStringArgs(self.args)

        job = self.getJobObject()

        if self.cmtsetup == []:
          raise ApplicationConfigurationError(None,'No cmt setup script given.')

        for arg in args:
          if arg == '-c':
            raise ApplicationConfigurationError(None,'Option "-c" given in args. You must use the configfile variable instead.')

        confopts = self.confopts


        # use input file from a "dataset"
        if job.inputdata == None:
            raise ApplicationConfigurationError(None,'The given config file requires an input file but the inputdata of the job is not defined.')
        infiles = job.inputdata.get_dataset_filenames()
        if len(infiles) < 1:
            raise ApplicationConfigurationError(None,'The given config file contains "inputfile" but not input file was given')
        if len(infiles) > 1:
            raise ApplicationConfigurationError(None,'The given config file contains "inputfile" but more than one input file was given')
        confopts.update({'inputfile':infiles[0]})

        # extract "run number" from an input filename
        if str(infiles[0]).isdigit():
            job.name = str(infiles[0])
            rrun = infiles[0]
            srun = 0
        else:
            mtch = re.search(r"(\d{8})-(\d{4})",os.path.basename(infiles[0]))
            if mtch:
                job.name = str(int(mtch.group(1)))+"-"+mtch.group(2)
                rrun     = mtch.group(1)
                srun     = mtch.group(2)
                irun     = int(rrun)
                if irun >= 1000:
                    rrun = str(irun % 1000)
            else:
                raise  ApplicationConfigurationError(None,'Can not extract run number')
        confopts.update({'run_number':str(int(rrun)),'subrun':str(int(srun))})

        # create config file
        cfg = ND280Configs.ND280Config('sandmc',confopts)
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
        #script += 'echo runND280 '+argsStr+' > oa_cs_mu_00003333-0033_numc_' + confopts['stage'] + '.root\n'
        script += 'rm -f [^o]*.dat\n'
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

allHandlers.add('runND280SandMC','LSF', RTHandler)
allHandlers.add('runND280SandMC','Local', RTHandler)
allHandlers.add('runND280SandMC','PBS', RTHandler)
allHandlers.add('runND280SandMC','SGE', RTHandler)
allHandlers.add('runND280SandMC','Condor', RTHandler)
allHandlers.add('runND280SandMC','LCG', LCGRTHandler)
allHandlers.add('runND280SandMC','gLite', gLiteRTHandler)
allHandlers.add('runND280SandMC','TestSubmitter', RTHandler)
allHandlers.add('runND280SandMC','Interactive', RTHandler)
allHandlers.add('runND280SandMC','Batch', RTHandler)
allHandlers.add('runND280SandMC','Cronus', RTHandler)
allHandlers.add('runND280SandMC','Remote', LCGRTHandler)
allHandlers.add('runND280SandMC','CREAM', LCGRTHandler)

