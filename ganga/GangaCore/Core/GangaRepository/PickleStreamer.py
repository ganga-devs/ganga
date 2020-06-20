"""
Converting the PickleStreamer to use json notations
"""

import json

try:
    import pickle as pickle
except:
    import pickle

from GangaCore.Utility.logging import getLogger

logger = getLogger()

def from_file(fobj):
    # return (pickle.load(fobj), [])
    return (json.load(fobj), [])


def to_file(obj, fileobj, ignore_subs=''):
    # DEBUG
    print("to_file", fileobj)
    try:
        # pickle.dump(obj, fileobj, 1)
        json.dump(obj, fileobj)
    except Exception as err:
        print("to_file error", obj, type(obj), fileobj)
        logger.error("Failed to Write: %s" % obj)
        logger.error("Err: %s" % err)
        raise

"""
['/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/prep.metadata/0xxx/0.index', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/prep.metadata/master.idx', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/prep/master.idx', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/jobs.metadata/0xxx/0.index', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/jobs.metadata/master.idx', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/jobs/master.idx', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/templates.metadata/0xxx/0.index', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/templates.metadata/master.idx', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/templates/master.idx', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/box.metadata/master.idx', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/box/master.idx', 
'/home/weakstation/gangadir/repository/weakstation/LocalJson/6.0/tasks/master.idx', 
]"""