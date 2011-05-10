from ROOT import TH1F, TBrowser, TCanvas
from Gaudi.Configuration import *

importOptions('$STDOPTS/LHCbApplication.opts')
#importOptions('$STDOPTS/DstDicts.opts')

appConf = ApplicationMgr( OutputLevel = INFO, AppName = 'Ex3' )
appConf.TopAlg += ["UnpackMCParticle","UnpackMCVertex"]

import GaudiPython
from Bender.MainMC import *

appMgr = GaudiPython.AppMgr()
evt = appMgr.evtsvc()

appMgr.run(1)
evt.dump()

appMgr.exit()
