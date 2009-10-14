from AthenaMC import *
from AthenaMCLCGRTHandler import *
from AthenaMCLocalRTHandler import *
from AthenaMCDatasets import *

loadPandaRTHandler=True
try:
    import GangaPanda.Lib.Panda
except SystemExit:
    from Ganga.Utility.logging import getLogger
    logger.error("Couldn't load Panda Client. Disabling AthenaMC-Panda RT Handler")
    loadPandaRTHandler=False
    pass
if loadPandaRTHandler:
    from AthenaMCPandaRTHandler import *

                                        
