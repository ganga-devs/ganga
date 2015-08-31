from Gaudi.Configuration import *
from DaVinci.Configuration import DaVinci
DaVinci().EvtMax = 100       # to be set by Ganga
DaVinci().DataType = "2010"
DaVinci().TupleFile = "DVHistos_1.root"
DaVinci().Lumi = True
DaVinci().Simulation = False
#DaVinci().MainOptions  = "$DAVINCIROOT/options/DVDC06TestStripping.opts"
