import os
import sys
import time
import datetime
import glob
import pickle
## NB parseCommandLine first then import Dirac!!
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
dirac = DiracLHCb()

