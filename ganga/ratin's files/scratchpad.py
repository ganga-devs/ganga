"""
NOTE: These files in this folder are temporary and are only meant as driver or observing scripts 
"""

from GangaCore.Core.GangaRepository.JStreamer import JsonDumper, to_database, from_database, connection
from GangaCore.GPIDev.Base.Proxy import addProxy, stripProxy, isType, getName
j = Job()
item = stripProxy(j)

# to_database(item, connection) # this is working nice
obj, error = from_database(connection, "_id", item.id)
# json = JsonDumper()
# json.parse(item)
# temp = json.job_json