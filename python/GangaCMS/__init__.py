import os
import Ganga.Utility.Config
import Ganga.Utility.logging
import platform

## CMSSW parameters
configCMSSW=Ganga.Utility.Config.makeConfig('CMSSW','Parameters for CMSSW')

dscrpt = 'The version CMSSW used for job submission.'
configCMSSW.addOption('CMSSW_VERSION','CMSSW_3_7_0',dscrpt)
dscrpt = 'The CMSSW setup script used for env configuration.'

config = Ganga.Utility.Config.getConfig('System')
ganga_pythonpath = config['GANGA_PYTHONPATH']

configCMSSW.addOption('CMSSW_SETUP','%s/GangaCMS/scripts/'%(ganga_pythonpath),dscrpt)
dscrpt = 'The location of the CMSSW Framework.'
configCMSSW.addOption('location','~/',dscrpt)

configMetrics = Ganga.Utility.Config.makeConfig('Metrics','List of desired metrics.')
dscrpt = 'The location of the metrics.cms list.'
configMetrics.addOption('location','%s/GangaCMS/metrics.ini'%(ganga_pythonpath),dscrpt)

dscrpt = 'The version CRAB used for job submission.'
configCMSSW.addOption('CRAB_VERSION','CRAB_2_7_5',dscrpt)

def getEnvironment( config = {} ):
    import sys
    import os.path
    import PACKAGE

    PACKAGE.standardSetup()

    arch = platform.machine()
    if not arch == 'x86_64':
        print 'GangaCMS> [ERROR] %s not supported. Different than 64 bits.'%(arch)
        return
    print 'GangaCMS> [INFO] not supported different OS than SLC5'

    config = Ganga.Utility.Config.getConfig('CMSSW')
    cmssw_version = config['CMSSW_VERSION']
    cmssw_setup = config['CMSSW_SETUP']
    crab_version = config['CRAB_VERSION']
    cmssw_setup_script = os.path.join(cmssw_setup,'CMSSW_generic.sh')
    if not os.path.exists(cmssw_setup_script):
        print 'GangaCMS> [ERROR] CMSSW setup script not found: "%s"'%(cmssw_setup_script)
        return

    location = config['location']

    cmsswhome = os.path.join(location,cmssw_version)
    if not os.path.exists(cmsswhome):
        print 'GangaCMS> [ERROR] CMSSW location not found: "%s"'%(cmsswhome)
        return

#    from Ganga.Utility.Shell import Shell
#    shell = Shell(cmssw_setup_script)   

    print 'GangaCMS> [INFO] getEnvironment : done'   
#    return shell.env
    return {}

def loadPlugins( config = {} ):
    import Lib.CRABTools
    import Lib.Utils
    import Lib.ConfParams

    crab_cfg_configs = {}

    for params in [Lib.ConfParams.CMSSW(),Lib.ConfParams.CRAB(),Lib.ConfParams.GRID(),Lib.ConfParams.USER()]:

      section = params.__class__.__name__
      crab_cfg_configs[section] = Ganga.Utility.Config.makeConfig('%s_CFG'%(section),'Parameters for %s at crab.cfg.'%(section))

      for k in params.schemadic.keys():
       crab_cfg_configs[section].addOption(k,None,'%s at crab.cfg'%(k))


    print 'GangaCMS> [INFO] loadPlugins : done'

