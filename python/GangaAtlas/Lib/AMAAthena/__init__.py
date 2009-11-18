from AMAAthena import *
from AMADriverConfig import *
from AMAAthenaLocalRTHandler import *
from AMAAthenaLCGRTHandler import *
from StagerJobSplitter import *
from StagerDataset import *
from SimpleStagerBroker import *

## introduce new DQ2 configuration variable
from Ganga.Utility.Config import makeConfig, ConfigError
config = getConfig('DQ2')
try:
    config.addOption('DQ2_LOCAL_SITE_ID', os.environ['DQ2_LOCAL_SITE_ID'], 'Sets the DQ2 local site id')
except KeyError:
    config.addOption('DQ2_LOCAL_SITE_ID', 'CERN-PROD_DATADISK', 'Sets the DQ2 local site id')
