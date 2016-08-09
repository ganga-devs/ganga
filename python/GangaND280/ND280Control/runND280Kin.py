################################################################################
# GangaND280 Project.
# Dima Vavilov
# Created 27/01/2016
################################################################################
"""@package ND280Control
Ganga module to execute RunAtmPitSim.exe from the atmPitSim package.
"""

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import getConfig

from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Lib.Registry.PrepRegistry import ShareRef
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.Core import ApplicationConfigurationError

import os, shutil, commands, re, random
from Ganga.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])

import ND280Configs

class runND280Kin(IApplication):
    """
    runND280Kin application running RunAtmPitSim.exe from the atmPitSim package
    for generating .kin files.
        app = runND280Kin()

    The required input for this module are:
        app.cmtsetup = '/home/me/myT2KWork/setup.sh'
        app.confopts = {'nd280ver':'v11r31','fluxfile':'/path/360DegOutputNew10MFlux.kin','nev':3500000}

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
    _name = 'runND280Kin'
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
        super(runND280Kin,self).__init__()


    def configure(self,masterappconfig):
        
        args = convertIntToStringArgs(self.args)

        job = self.getJobObject()

        if self.cmtsetup == []:
          raise ApplicationConfigurationError(None,'No cmt setup script given.')

        infiles = job.inputdata.get_dataset_filenames()
        if len(infiles) <> 1:
           raise ApplicationConfigurationError(None,'Wrong Dataset values')
        jn = "%08d" % int(infiles[0])
        outConf = ''
        outConf += "# Automatically generated config file\n\n"
        outConf += "/atmt2k/step/outputFileName corsika_atmpitsim_"+jn+".root\n"
        outConf += "/atmt2k/pga/inputFileName "+self.confopts['fluxfile']+"\n"
        outConf += "/atmt2k/pga/isOyamaFlux false\n"
        outConf += "/atmt2k/pga/inputRandSeed "+str(random.randint(1,9999999))+"\n\n"
        outConf += "/run/verbose 0\n"
        outConf += "/event/verbose 0\n"
        outConf += "/tracking/verbose 0\n"
        outConf += "/run/beamOn "+self.confopts['nev']+"\n"

        mac = "corsika_atmpitsim_"+jn+".mac"
        job.getInputWorkspace().writefile(FileBuffer(mac,outConf),executable=0)

        argsStr = ' '.join(args)
        # Create the bash script and put it in input dir.
        script = '#!/bin/bash\n'
        for f in self.cmtsetup:
            script += 'source '+f+'\n'
        script += 'cd '+job.outputdir+'\n'
        script += 'RunAtmPitSim.exe '+argsStr+' '+os.path.join(job.inputdir,mac)+'\n'

        script += 'mac=$(dirname $(which RunAtmPitSim.exe))/../app/ntuple_2_nuance.C\n'
        script += 'cp $mac .\n'
        script += 'root -l -b <<EOF\n'
        script += '.L ntuple_2_nuance.C+\n'
        script += 'totxt("corsika_atmpitsim_'+jn+'");\n'
        script += '.q\n'
        script += 'EOF\n'

        script += 'mv NoMuons.txt NoMuons_'+jn+'.txt\n'
        script += 'mv Config.conf Config_'+jn+'.conf\n'
        job.getInputWorkspace().writefile(FileBuffer('runND280.sh',script),executable=1)

        self._scriptname = job.inputdir+'runND280.sh'

        # Job name given
        job.name = jn


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

allHandlers.add('runND280Kin','LSF', RTHandler)
allHandlers.add('runND280Kin','Local', RTHandler)
allHandlers.add('runND280Kin','PBS', RTHandler)
allHandlers.add('runND280Kin','SGE', RTHandler)
allHandlers.add('runND280Kin','Condor', RTHandler)
allHandlers.add('runND280Kin','LCG', LCGRTHandler)
allHandlers.add('runND280Kin','gLite', gLiteRTHandler)
allHandlers.add('runND280Kin','TestSubmitter', RTHandler)
allHandlers.add('runND280Kin','Interactive', RTHandler)
allHandlers.add('runND280Kin','Batch', RTHandler)
allHandlers.add('runND280Kin','Cronus', RTHandler)
allHandlers.add('runND280Kin','Remote', LCGRTHandler)
allHandlers.add('runND280Kin','CREAM', LCGRTHandler)

