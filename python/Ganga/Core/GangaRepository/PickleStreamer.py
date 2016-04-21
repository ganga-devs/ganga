try:
    import cPickle as pickle
except:
    import pickle

from Ganga.Utility.logging import getLogger

logger = getLogger()

def from_file(fobj):
    return (pickle.load(fobj), [])


def to_file(obj, fileobj, ignore_subs=''):
    try:
        pickle.dump(obj, fileobj, 1)
    except Exception as err:
        logger.error("Failed to Write: %s" % obj)
        logger.error("Err: %s" % err)
        raise
