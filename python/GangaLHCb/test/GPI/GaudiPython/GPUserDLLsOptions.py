from Gaudi.Configuration import *

importOptions('$STDOPTS/LHCbApplication.opts')
importOptions('$STDOPTS/DstDicts.opts')

importOptions('DVTutorial_1.py')
importOptions('Bs2JpsiPhi2008.py')

import GaudiPython

appMgr = GaudiPython.AppMgr()
evt = appMgr.evtsvc()

while True:
    appMgr.run(1)
    if evt['/Event/Rec/Header'] is None: break

appMgr.exit()

