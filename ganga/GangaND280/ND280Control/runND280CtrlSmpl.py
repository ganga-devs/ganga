################################################################################
# GangaND280 Project.
# Anthony Hillairet
# Created 11/01/2014
################################################################################
"""@package ND280Control
Ganga module to create control samples using runND280ControlSample from the nd280Control package.
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

class runND280CtrlSmpl(IPrepareApp):
    """
    runND280CtrlSmpl application running runND280ControlSample from nd280Control to produce control samples defined in oaControlSample.

    The required input for this module are:
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
        app.configfile = 'myControlSamples.cfg'

    NOTE: Ganga will run a bash script so the CMT setup script must be in sh/bash.

    This module can run only the skimming of reco files, or skim the reco files and then analyze the resulting files with oaAnalysis on the fly.
    By default the oaAnalysis step is turned off. To turn it on, use the following:
        app.runoaanalysis = True

    You can pass command line arguments to RunOAAnalysis.exe this way:
        app.oaanalysisargs = ['-n',10]

    """
    _schema = Schema(Version(1,0), {
        'args' : SimpleItem(defvalue=[],typelist=['str','GangaCore.GPIDev.Lib.File.File.File','int'],sequence=1,strict_sequence=0,doc="List of arguments for the executable. Arguments may be strings, numerics or File objects."),
        'cmtsetup' : SimpleItem(defvalue=None,doc='Setup script in bash to set up cmt and the cmt package of the executable.', typelist=['str','type(None)']),
        'configfile' : SimpleItem(defvalue=None,doc='Filename of the nd280Control config file.', typelist=['str','type(None)']),
        'runoaanalysis' : SimpleItem(defvalue=False,doc='Turn on/off the oaAnalysis processing of the reco control samples.', typelist=['bool']),
        'oaanalysisargs' : SimpleItem(defvalue=[],typelist=['str','int'],sequence=1,strict_sequence=0,doc="List of arguments for the oaAnalysis stage. Arguments may be strings, numerics."),
        'env' : SimpleItem(defvalue={},typelist=['str'],doc='Environment'),
        'is_prepared' : SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, typelist=['type(None)','bool'],protected=0,comparable=1,doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        } )
    _category = 'applications'
    _name = 'runND280CtrlSmpl'
    _exportmethods = ['prepare']
    _GUIPrefs = [ { 'attribute' : 'args', 'widget' : 'String_List' },
                  { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                  { 'attribute' : 'configfile', 'widget' : 'String' },
                  { 'attribute' : 'env', 'widget' : 'DictOfString' } ]

    _GUIAdvancedPrefs = [ { 'attribute' : 'args', 'widget' : 'String_List' },
                          { 'attribute' : 'cmtsetup', 'widget' : 'String' },
                          { 'attribute' : 'configfile', 'widget' : 'String' },
                          { 'attribute' : 'env', 'widget' : 'DictOfString' } ]
    exe = 'runND280CtrlSmpl'

    def __init__(self):
        super(runND280CtrlSmpl,self).__init__()


    def configure(self,masterappconfig):
        
        args = convertIntToStringArgs(self.args)
        anaargs = convertIntToStringArgs(self.oaanalysisargs)

        job = self.getJobObject()

        if self.cmtsetup is None:
          raise ApplicationConfigurationError('No cmt setup script given.')

        for arg in args:
          if arg == '-c':
            raise ApplicationConfigurationError('Option "-c" given in args. You must use the configfile variable instead.')

        # setup the config file for this job
        if self.configfile is None:
          raise ApplicationConfigurationError('No config file given. Use args list or configfile field.')
        # check if given config file exists
        if not os.path.exists(self.configfile):
          raise ApplicationConfigurationError('The given config file "'+self.configfile+'" was not found.')
        if not os.path.isfile(self.configfile):
          raise ApplicationConfigurationError('The given config file "'+self.configfile+'" is not a file.')

        if job.inputdata is None:
          raise ApplicationConfigurationError('The inputdata of the job is not defined.')
        infiles = job.inputdata.get_dataset_filenames()
        if len(infiles) < 1:
          raise ApplicationConfigurationError('The inputdata is empty.')

        # Right here, take the input config file and change it as needed
        # If found inputfile, just put the first file in the inputdata
        # If this is inputfile_list, then it is in cherry pick so we can put all the files from inputdata
        inConf = open(self.configfile)
        outConf = ''

        for line in inConf:
            inputfilesfnd = re.match(r"^inputfiles\s*=", line)
            if inputfilesfnd:
              line = 'inputfiles = ' + ' '.join(infiles) + '\n'
            
            outConf += line
        job.getInputWorkspace().writefile(FileBuffer('nd280Config.cfg',outConf),executable=0)

        args.append('-c')
        args.append(job.inputdir+'nd280Config.cfg')

        argsStr = ' '.join(args)
        anaargsStr = ' '.join(anaargs)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        script += 'source '+self.cmtsetup+'\n'
        script += 'runND280ControlSample '+argsStr+'\n'
        if self.runoaanalysis:
            script += "for Input in `ls *.root`\ndo\n"
            script += "    Output=`echo $Input | sed 's/_reco_/_anal_/g'`\n"
            script += "    RunOAAnalysis.exe "+anaargsStr+" -o $Output $Input\ndone\n"
        job.getInputWorkspace().writefile(FileBuffer('runND280CtrlSmpl.sh',script),executable=1)

        self._scriptname = job.inputdir+'runND280CtrlSmpl.sh'

        return (None,None)



config = getConfig('defaults_runND280CtrlSmpl') #_Properties
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

allHandlers.add('runND280CtrlSmpl','LSF', RTHandler)
allHandlers.add('runND280CtrlSmpl','Local', RTHandler)
allHandlers.add('runND280CtrlSmpl','PBS', RTHandler)
allHandlers.add('runND280CtrlSmpl','SGE', RTHandler)
allHandlers.add('runND280CtrlSmpl','Slurm', RTHandler)
allHandlers.add('runND280CtrlSmpl','Condor', RTHandler)
allHandlers.add('runND280CtrlSmpl','LCG', LCGRTHandler)
allHandlers.add('runND280CtrlSmpl','gLite', gLiteRTHandler)
allHandlers.add('runND280CtrlSmpl','TestSubmitter', RTHandler)
allHandlers.add('runND280CtrlSmpl','Interactive', RTHandler)
allHandlers.add('runND280CtrlSmpl','Batch', RTHandler)
allHandlers.add('runND280CtrlSmpl','Cronus', RTHandler)
allHandlers.add('runND280CtrlSmpl','Remote', LCGRTHandler)
allHandlers.add('runND280CtrlSmpl','CREAM', LCGRTHandler)

