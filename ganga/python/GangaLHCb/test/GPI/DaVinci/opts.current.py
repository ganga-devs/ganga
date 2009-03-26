from Gaudi.Configuration import *
from DaVinci.Configuration import DaVinci
DaVinci().EvtMax = -1       # to be set by Ganga 
# DaVinci().DataType = "2008" # Default is "DC06" -  to be set by Ganga 
DaVinci().Simulation   = True
DaVinci().MainOptions  = "$DAVINCIROOT/options/DVDC06TestStripping.opts"


 
