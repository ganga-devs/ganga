"""
NOTE: These files in this folder are temporary and are only meant as driver or observing scripts 
"""

# Importing the proxy strippers
from GangaCore.Core.exceptions import GangaException
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy, isType, getName

from GangaCore.GPIDev.Lib.GangaList.GangaList import GangaList, makeGangaListByRef

# config_scope is namespace used for evaluating simple objects (e.g. File, datetime, SharedDir)
from GangaCore.Utility.Config import config_scope

from GangaCore.Utility.Plugin import PluginManagerError, allPlugins

from GangaCore.GPIDev.Base.Objects import GangaObject, ObjectMetaclass
from GangaCore.GPIDev.Schema import Schema, Version
from GangaCore.GPIDev.Lib.GangaList.GangaList import makeGangaList


from GangaCore.GPIDev.Lib.Registry.RegistrySlice import RegistrySlice
from GangaCore.GPIDev.Lib.Registry.RegistrySliceProxy import RegistrySliceProxy

"""
Creating the a job
"""
j = jobs[-1]
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy, isType, getName
item = stripProxy(j)
from GangaCore.Core.GangaRepository.JStreamer import JsonDumper
json = JsonDumper()
json.parse(item)
temp = json.job_json