from Ganga.Lib.Localhost.Localhost import Localhost
from Ganga.Lib.Interactive.Interactive import Interactive
from Ganga.Lib.Batch.Batch import *

def master_prepare(self, masterjobconfig):
    if masterjobconfig:
        return [f.name for f in masterjobconfig.getSandboxFiles()]
    return []
   
setattr(Localhost,'master_prepare',master_prepare)
setattr(Interactive,'master_prepare',master_prepare)
setattr(PBS,'master_prepare',master_prepare)
setattr(SGE,'master_prepare',master_prepare)
##localhost.master_prepare=master_prepare
