from Ganga.Lib.Localhost.Localhost import Localhost

def master_prepare(self, masterjobconfig):
    if masterjobconfig:
        return [f.name for f in masterjobconfig.getSandboxFiles()]
    return []
   
setattr(Localhost,'master_prepare',master_prepare)
##localhost.master_prepare=master_prepare
