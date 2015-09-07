from DIRAC.Core.Base.Script import parseCommandLine
from DIRAC.Interfaces.API.Dirac import Dirac
import os
parseCommandLine()
dirac=Dirac()
dirac.getFile('###LFN###', os.getcwd())
