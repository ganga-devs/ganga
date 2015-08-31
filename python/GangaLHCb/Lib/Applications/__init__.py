import os
from Ganga.GPIDev.Lib.Tasks.TaskApplication import taskify, task_map
from Ganga.Runtime.GPIexport import exportToGPI
from Ganga.GPIDev.Base.Proxy import GPIProxyClassFactory

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
import GangaLHCb.Lib.Applications.AppsBaseUtils
from Ganga.Utility.logging import getLogger

from GaudiPython import GaudiPython
from Bender import Bender

# Add any additional Packages required by the user in the .gangarc file
from Ganga.Utility.Config import getConfig

logger = getLogger()

config = getConfig('LHCb')
user_added = config['UserAddedApplications']
user_apps = user_added.split(':')
if user_apps == user_added and len(user_added) > 0:
    AppsBaseUtils.addNewLHCbapp(user_apps)
for app in user_apps:
    if len(app) > 0:
        AppsBaseUtils.addNewLHCbapp(app)

f = open(os.path.join(os.path.dirname(__file__), 'AppsBase.py'), 'r')
cls = f.read()
f.close()
all_apps = ''
for app in AppsBaseUtils.available_apps():
    if app in dir():
        continue
    app = str(app)
    this_class = cls.replace('AppName', app)
    this_task = '%sTask = taskify(%s,"%sTask")' % (app, app, app)
    this_map = 'task_map["%s"] = %sTask' % (app, app)
    this_app = this_class + str('\n\n') + this_task + str('\n\n') + this_map

    all_apps = all_apps + str('\n\n') + this_app
    #exec(this_exec, all_global, all_local)
    logger.debug("Adding %s" % str(app))

exec(all_apps)

logger.debug("Fin")

#    exec(cls.replace('AppName', app))
# dont seem necessary
#    exec('%sTask = taskify(%s,"%sTask")' %(app, app, app))
#    exec('task_map["%s"] = %sTask' %(app, app))
###    exec('task_map["%s"] = %s' %(app,app))
##    obj_class = GPIProxyClassFactory( "%s" % app, locals()[app] )
##    exportToGPI( "%s" % app, obj_class(), '' )

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
GaudiPythonTask = taskify(GaudiPython, "GaudiPythonTask")
task_map["GaudiPython"] = GaudiPythonTask
#task_map["GaudiPython"] = GaudiPython

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
BenderTask = taskify(Bender, "BenderTask")
task_map["Bender"] = BenderTask

