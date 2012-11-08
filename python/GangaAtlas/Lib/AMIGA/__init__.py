from Ganga.Utility.logging import getLogger
from Ganga.Utility.Setup import checkPythonVersion
import sys
logger = getLogger()

if sys.hexversion > 0x2040000:
    from AMIDataset import *
else:
    logger.warning("AMI not properly set up. Set athena to access AMI from this ganga session.")
