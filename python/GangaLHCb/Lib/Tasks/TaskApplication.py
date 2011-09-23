########################################################################
# File : TaskApplication.py
########################################################################

from Ganga.GPIDev.Lib.Tasks.TaskApplication import taskify

from GangaLHCb.Lib.Gaudi.Gaudi import *
#GaudiTask = taskify(Gaudi,"GaudiTask")
#DaVinciTask = taskify(DaVinci,"DaVinciTask")

#must take care of "taskifying" all possible apps
from GangaLHCb.Lib.Gaudi.GaudiUtils import available_apps
from Ganga.GPIDev.Lib.Tasks.TaskApplication import task_map
## from Ganga.GPIDev.Base.Proxy import *
## from Ganga.GPIDev.Schema import *

for app in available_apps():
    exec('%sTask = taskify(%s,"%sTask")' %(app,app,app))
#    exec('smajor = %sTask._schema.version.major'%app)
#    exec('sminor = %sTask._schema.version.minor'%app)
#    exec('schema_items = {\'extra\':ComponentItem(\'extras\',defvalue=GaudiExtras(),load_default=1,Hidden=1,protected=0,optional=1,copyable=1,doc=\'Used to pass extra info from Gaudi apps to the RT-handler.\')}.items()')
#    exec('%sTask._schema = Schema()')

#    exec('%sTask._schema = Schema(Version(smajor,sminor), dict(%sTask._schema.datadict.items() + schema_items ))'%(app,app))
#    exec('%sTask._name = \'%sTask\''%(app,app))
    exec('task_map["%s"] = %sTask' %(app,app))
#    exec('print \'ALEX HERE - \',%sTask._schema.getDefaultValue(\'extra\')'%app)

from GangaLHCb.Lib.Gaudi.GaudiPython import GaudiPython
GaudiPythonTask = taskify(GaudiPython,"GaudiPythonTask")
