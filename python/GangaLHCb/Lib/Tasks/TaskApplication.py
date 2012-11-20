########################################################################
# File : TaskApplication.py
########################################################################

from Ganga.GPIDev.Lib.Tasks.TaskApplication import taskify
from Ganga.GPIDev.Lib.Tasks.TaskApplication import task_map

## Must take care of "taskifying" all possible apps
#from GangaLHCb.Lib.Applications.AppsBase import *
#from GangaLHCb.Lib.Applications.AppsBaseUtils import available_apps
import os
from GangaLHCb.Lib.Applications.AppsBaseUtils import available_apps
f = open(os.path.join(os.path.dirname(__file__),'..','Applications','AppsBase.py'),'r')
cls = f.read()
f.close()

## for app in available_apps():    
##     exec(cls.replace('AppName','%s'%app))

for app in available_apps():
    exec(cls.replace('AppName','%s'%app))
    exec('%sTask = taskify(%s,"%sTask")' %(app,app,app))
    exec('task_map["%s"] = %sTask' %(app,app))


from GangaLHCb.Lib.Applications.GaudiPython import GaudiPython

GaudiPythonTask = taskify(GaudiPython,"GaudiPythonTask")
task_map["GaudiPython"] = GaudiPythonTask

from GangaLHCb.Lib.Applications.Bender import Bender

BenderTask = taskify(Bender,"BenderTask")
task_map["Bender"] = BenderTask
