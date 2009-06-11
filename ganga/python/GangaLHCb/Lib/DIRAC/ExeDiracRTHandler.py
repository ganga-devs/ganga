#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from DiracScript import *

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class ExeDiracRTHandler(IRuntimeHandler):
    """The runtime handler to run plain executables on the Dirac backend"""

    def master_prepare(self,app,appconfig):        
        inputsandbox = app._getParent().inputsandbox[:]
        c = StandardJobConfig('',inputsandbox,[],[],None)
        return c

    def prepare(self,app,appconfig,appmasterconfig,jobmasterconfig):
        j = app.getJobObject()
        c = StandardJobConfig(app.exe,[],app.args,j.outputsandbox,app.env)

        dirac_script = DiracScript()
        dirac_script.job_type = 'LHCbJob()'
        dirac_script.exe = DiracExe(app.exe,app.args)
        dirac_script.output_sandbox = j.outputsandbox[:]

        if j.inputdata: dirac_script.inputdata = DiracInputData(j.inputdata)
          
        if j.outputdata:
            dirac_script.outputdata = [f.name for f in j.outputdata.files]

        c.script = dirac_script        
        return c

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
