try:
    import pickle as pickle
except:
    import pickle

from GangaCore.Utility.logging import getLogger
import cloudpickle
logger = getLogger()

def from_file(fobj):
    return (pickle.load(fobj), [])


def to_file(obj, fileobj, ignore_subs=''):
    try:
        cloudpickle.dump(obj, fileobj, 1)
    except Exception as err:
        logger.error(f"Failed to Write: {obj}")
        logger.error(f"Err: {err}")
        raise
