## NB parseCommandLine first then import Dirac!!
import os
import sys
import time
import datetime
import glob
import pickle
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
dirac = Dirac()

