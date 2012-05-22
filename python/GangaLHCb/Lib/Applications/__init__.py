import os
from Ganga.GPIDev.Lib.Tasks.TaskApplication import taskify
from Ganga.GPIDev.Lib.Tasks.TaskApplication import task_map

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
from GangaLHCb.Lib.Applications.AppsBaseUtils import available_apps
f = open(os.path.join(os.path.dirname(__file__),'AppsBase.py'),'r')
cls = f.read()
f.close()
for app in available_apps():
    if app in dir(): continue
    exec(cls.replace('AppName',app))
    exec('%sTask = taskify(%s,"%sTask")' %(app,app,app))
    exec('task_map["%s"] = %sTask' %(app,app))
##    exec('task_map["%s"] = %s' %(app,app))


#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
from GaudiPython import *
GaudiPythonTask = taskify(GaudiPython,"GaudiPythonTask")
task_map["GaudiPython"] = GaudiPythonTask

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
from Bender import *
BenderTask = taskify(Bender,"BenderTask")
task_map["Bender"] = BenderTask
