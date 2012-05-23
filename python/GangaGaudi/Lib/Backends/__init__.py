from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.Lib.Localhost.Localhost import Localhost
from Ganga.Lib.Interactive.Interactive import Interactive
from Ganga.Lib.Batch.Batch import *

def master_prepare(self, masterjobconfig):
    def filt(sharedsandbox):
        if sharedsandbox:
            def shareboxfilter(item):
                return item.name.find(self.getJobObject().application.is_prepared.name) is not -1
            return shareboxfilter

        def nonshareboxfilter(item):
            return item.name.find(self.getJobObject().application.is_prepared.name) is -1
        return nonshareboxfilter

    
    if masterjobconfig:

        inputsandbox  = [f.name for f in filter(filt(True) , masterjobconfig.getSandboxFiles())]
        sjc = StandardJobConfig(inputbox=filter(filt(False), masterjobconfig.getSandboxFiles()))
        inputsandbox += super(type(self),self).master_prepare(sjc)
        return inputsandbox
    return []
   
setattr(Localhost,'master_prepare',master_prepare)
setattr(Interactive,'master_prepare',master_prepare)
setattr(PBS,'master_prepare',master_prepare)
setattr(SGE,'master_prepare',master_prepare)
##localhost.master_prepare=master_prepare
