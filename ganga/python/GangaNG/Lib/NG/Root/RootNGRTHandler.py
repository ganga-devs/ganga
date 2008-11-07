# $Id: RootNGRTHandler.py,v 1.1 2008-07-17 16:41:29 moscicki Exp $

from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers


class RootNGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from GangaNG.Lib.NG import NGJobConfig
        from Ganga.Lib.Root import downloadWrapper
        
        runScript, inputsandbox, rootenv = downloadWrapper(app)
        
        return NGJobConfig(runScript,inputsandbox,[],
                            app._getParent().outputsandbox,rootenv)
                            

allHandlers.add('Root','NG',RootNGRTHandler)

# $Log: not supported by cvs2svn $
# Revision 1.2  2007/03/19 18:09:37  bsamset
# Several bugfixes, added arc middleware as external package
#
