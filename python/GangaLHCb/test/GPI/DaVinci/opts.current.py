from Gaudi.Configuration import *
from DaVinci.Configuration import DaVinci
DaVinci().EvtMax = -1       # to be set by Ganga 
DaVinci().DataType = "MC09" 
DaVinci().Simulation   = True
#DaVinci().MainOptions  = "$DAVINCIROOT/options/DVDC06TestStripping.opts"


 
