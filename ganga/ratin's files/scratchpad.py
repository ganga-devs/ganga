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

from GangaCore.Core.GangaRepository.VStreamer import to_file, compositeAttribute
# from GangaCore.Core.Lib.Job import Job
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy, isType, getName
to_file(stripProxy(j), open("/home/dumbmachine/work/gsoc/ganga/ganga/ratin's files/temp.xml", "w"))


item = stripProxy(j)


this_object = item


temp = compositeAttribute("job", item)

if isinstance(item, list):
    objectList = [stripProxy(element) for element in item]
elif isinstance(item, tuple):
    objectList = [stripProxy(element) for element in item]
elif isType(item, RegistrySliceProxy) or isType(item, RegistrySlice):
    objectList = [stripProxy(element) for element in item]
elif isType(item, GangaList):
    objectList = [stripProxy(element) for element in item]
else:
    objectList = [item]

nObject = 0

node = item

# def value_from_node(node):
#     """Will return a dict of values for the node
#     """

