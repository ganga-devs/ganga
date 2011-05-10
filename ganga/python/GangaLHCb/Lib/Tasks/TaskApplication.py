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

for app in available_apps():
    exec('%sTask = taskify(%s,"%sTask")' %(app,app,app))
    exec('task_map["%s"] = %sTask' %(app,app))

from GangaLHCb.Lib.Gaudi.GaudiPython import GaudiPython
GaudiPythonTask = taskify(GaudiPython,"GaudiPythonTask")
