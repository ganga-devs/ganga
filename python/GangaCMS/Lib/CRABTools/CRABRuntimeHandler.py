#
# CRAB RuntimeHandler
#
# 08/06/10 @ ubeda
#


from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

class CRABRuntimeHandler(IRuntimeHandler):
  
    def __init__(self):
        print ''
        #super(CRABRuntimeHandler,self).__init__()
    
    def master_prepare(self, app, appconfig):
        
        return None

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):

        return None

allHandlers.add('CRABApp','CRABBackend', CRABRuntimeHandler)





