## NB parseCommandLine first then import Dirac!!
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.DiracAdmin import DiracAdmin
dirac = Dirac()

