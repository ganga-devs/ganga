from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

class RTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

        c = StandardJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)

        #c.monitoring_svc = mc['Executable']

        return c


class LCGRTHandler(IRuntimeHandler):
    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        from Ganga.Lib.LCG import LCGJobConfig

        return LCGJobConfig(app.exe,app._getParent().inputsandbox,app.args,app._getParent().outputsandbox,app.env)

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers

allHandlers.add('Executable','Gridway', RTHandler)
allHandlers.add('Executable','InterGrid', LCGRTHandler)
