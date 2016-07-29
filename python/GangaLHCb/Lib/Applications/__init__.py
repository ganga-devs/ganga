from __future__ import absolute_import
import os

from Ganga.Runtime.GPIexport import exportToGPI
from Ganga.GPIDev.Base.Proxy import GPIProxyClassFactory

#/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
from GangaLHCb.Lib.Applications import AppsBaseUtils
from Ganga.Utility.logging import getLogger

from .GaudiPython import GaudiPython

from .Bender import Bender
from .BenderScript import BenderScript
from .Ostap import Ostap

from .GaudiExec import GaudiExec

# Add any additional Packages required by the user in the .gangarc file
from Ganga.Utility.Config import getConfig

logger = getLogger()
logger.debug("User Added Apps")

config = getConfig('LHCb')
user_added = config['UserAddedApplications']
user_apps = user_added.split(':')
if user_apps == user_added and len(user_added) > 0:
    AppsBaseUtils.addNewLHCbapp(user_apps)
for app in user_apps:
    if len(app) > 0:
        AppsBaseUtils.addNewLHCbapp(app)

logger.debug("Constructing AppsBase Apps")
f = open(os.path.join(os.path.dirname(__file__), 'AppsBase.py'), 'r')
cls = f.read()
f.close()
all_apps = ''
for app in AppsBaseUtils.available_apps():
    if app in dir():
        continue
    app = str(app)
    this_app = cls.replace('AppName', app)
    all_apps = all_apps + str('\n\n') + this_app
    #exec(this_exec, all_global, all_local)
    logger.debug("Adding %s" % str(app))

logger.debug("Adding apps")
modules= compile(all_apps, '<string>', 'exec')
exec modules

logger.debug("Fin")


