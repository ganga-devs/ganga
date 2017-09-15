import os
## NB parseCommandLine first then import Dirac!!
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
dirac=Dirac()
dirac.getFile('###LFN###', os.getcwd())
