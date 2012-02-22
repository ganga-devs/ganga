###############################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: SFrameARALocalRTHandler.py,v 1.1 2008-11-19 15:43:01 mbarison Exp $
###############################################################################
# SFrameARA Local Runtime Handler
#
# ATLAS/ARDA

import os, socket, pwd, commands, re, string

from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *
from Ganga.GPIDev.Lib.File import *
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

from GangaAtlas.Lib.ATLASDataset import ATLASDataset, isDQ2SRMSite, getLocationsCE, getIncompleteLocationsCE, getIncompleteLocations
from GangaAtlas.Lib.ATLASDataset import ATLASCastorDataset
from GangaAtlas.Lib.ATLASDataset import ATLASLocalDataset
from GangaAtlas.Lib.ATLASDataset import DQ2Dataset

from GangaAtlas.Lib.Athena       import AthenaLocalRTHandler
from GangaSFrame.Lib.SFrame      import SFrameAppLocalRTHandler

from Ganga.Utility.Config import getConfig, makeConfig, ConfigError
from Ganga.Utility.logging import getLogger

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Credentials import GridProxy
  
class SFrameARALocalRTHandler(AthenaLocalRTHandler, SFrameAppLocalRTHandler):
    """SFrameARA Local Runtime Handler"""

    def __init__(self):
        #super(SFrameARALocalRTHandler, self).__init__()
        
        return
    
    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):
        """prepare the subjob specific configuration"""

        #sjc = super(SFrameARALocalRTHandler, self).prepare(app,appsubconfig,appmasterconfig,jobmasterconfig)
        a_sjc = AthenaLocalRTHandler.prepare(self, app,appsubconfig,appmasterconfig,jobmasterconfig)
        s_sjc = SFrameAppLocalRTHandler.prepare(self, app,appsubconfig,appmasterconfig,jobmasterconfig)       



        # Now we need to combine the two configurations

        exe = os.path.join(os.path.dirname(__file__),'run-sframeARA-local.sh')

        # we need to merge the inputbox
        inputbox = a_sjc.inputbox + s_sjc.inputbox

        # and the environment
        environment = a_sjc.env
        environment.update(s_sjc.env)

        # the output inbox is updated already
        outputbox = s_sjc.outputbox

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)

                 

    def master_prepare( self, app, appconfig ):
        """Prepare the master job"""

        #sjc = super(SFrameARALocalRTHandler, self).master_prepare(app,appconfig)
        a_sjc = AthenaLocalRTHandler.master_prepare(self,app,appconfig)
        s_sjc = SFrameAppLocalRTHandler.master_prepare(self,app,appconfig)

        exe = os.path.join(os.path.dirname(__file__),'run-sframeARA-local.sh')

        # we need to merge the inputbox
        inputbox = a_sjc.inputbox + s_sjc.inputbox

        # and the environment
        environment = a_sjc.env
        environment.update(s_sjc.env)

        # the output inbox is updated already
        outputbox = s_sjc.outputbox

        return StandardJobConfig(File(exe), inputbox, [], outputbox, environment)




allHandlers.add('SFrameARA', 'Local', SFrameARALocalRTHandler)
allHandlers.add('SFrameARA', 'LSF'  , SFrameARALocalRTHandler)
allHandlers.add('SFrameARA', 'PBS'  , SFrameARALocalRTHandler)
allHandlers.add('SFrameARA', 'SGE'  , SFrameARALocalRTHandler)

config = getConfig('Athena')
configDQ2 = getConfig('DQ2')
configLCG = getConfig('LCG')
logger = getLogger()


#$Log: not supported by cvs2svn $
