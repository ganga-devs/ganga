import os
from Ganga.GPIDev.Lib.Tasks.TaskApplication import taskify
from Ganga.GPIDev.Lib.Tasks.TaskApplication import task_map

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
import GangaLHCb.Lib.Applications.AppsBaseUtils

##  Add any additional Packages required by the user in the .gangarc file
from Ganga.Utility.Config import getConfig
config = getConfig('LHCb')
user_added = config['UserAddedApplications']
user_apps = user_added.split(':')
if user_apps == user_added and len(user_added) > 0:
    AppsBaseUtils.addNewLHCbapp( user_apps )
for app in user_apps:
    if len(app) > 0:
        AppsBaseUtils.addNewLHCbapp( app )

f = open(os.path.join(os.path.dirname(__file__),'AppsBase.py'),'r')
cls = f.read()
f.close()
for app in AppsBaseUtils.available_apps():
    if app in dir(): continue
    exec(cls.replace('AppName',app))
##     # dont seem necessary
    exec('%sTask = taskify(%s,"%sTask")' %(app,app,app))
    exec('task_map["%s"] = %sTask' %(app,app))
##    exec('task_map["%s"] = %s' %(app,app))


#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
from GaudiPython import *
GaudiPythonTask = taskify(GaudiPython,"GaudiPythonTask")
task_map["GaudiPython"] = GaudiPythonTask
#task_map["GaudiPython"] = GaudiPython

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
from Bender import *
BenderTask = taskify(Bender,"BenderTask")
task_map["Bender"] = BenderTask
#task_map["Bender"] = Bender
