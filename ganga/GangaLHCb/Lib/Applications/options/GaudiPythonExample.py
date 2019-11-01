import atexit
from GaudiPython.Bindings import gbl, AppMgr, Helper
from AnalysisPython import Dir, Functors
from ROOT import TCanvas
from LHCbAlgs.Configuration import *

lhcbApp = LHCbApp(DDDBtag='default',
                  CondDBtag='default',
                  DataType='2010',
                  Simulation=False)


appMgr = AppMgr(outputlevel=4)
# appMgr.config( files = ['$GAUDIPOOLDBROOT/options/GaudiPoolDbRoot.opts'])
appMgr.ExtSvc += ['DataOnDemandSvc']
appMgr.initialize()

evt = appMgr.evtsvc()

appMgr.run(1)
evt.dump()

atexit.register(appMgr.exit)
