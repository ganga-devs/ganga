from ROOT import TCanvas
from LHCbAlgs.Configuration import *

lhcbApp = LHCbApp(DDDBtag='default',
                  CondDBtag='default',
                  DataType='2010',
                  Simulation=False)

from AnalysisPython import Dir, Functors
from GaudiPython.Bindings import gbl, AppMgr, Helper

appMgr = AppMgr(outputlevel=4)
#appMgr.config( files = ['$GAUDIPOOLDBROOT/options/GaudiPoolDbRoot.opts'])
appMgr.ExtSvc += ['DataOnDemandSvc']
appMgr.initialize()

evt = appMgr.evtsvc()

appMgr.run(1)
evt.dump()

import atexit
atexit.register(appMgr.exit)
