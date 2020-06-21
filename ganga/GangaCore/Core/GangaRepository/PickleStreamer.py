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
    return (pickle.load(fobj), [])


def to_file(obj, fileobj, ignore_subs=''):
    try:
        pickle.dump(obj, fileobj, 1)
    except Exception as err:
        logger.error(f"Failed to Write: {obj} to {fileobj}")
        logger.error(f"Err: {err}")
        raise

def json_pickle_from_file(fobj):
    return (json.load(fobj), [])


def json_pickle_to_file(obj, fileobj, ignore_subs=''):
    try:
        json.dump(obj, fileobj)
    except Exception as err:
        logger.error(f"json_pickle_to_file Failed to Write: {obj} to {fileobj}")
        logger.error(f"Err: {err}")
        raise
