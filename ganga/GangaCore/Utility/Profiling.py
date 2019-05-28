from GangaCore.Utility.Config import getConfig

import os
from memory_profiler import profile
import cProfile
import time

# creating a timestamp
timestr = time.strftime("-%Y%m%d-%H%M%S")

# Obtaining the config from .gangarc file
c = getConfig('Configuration')

path = os.path.join(c['gangadir'], 'logs')


# A helper function to make the directories for storing log files
def _makedir(path):
    try:
        if not os.path.exists(path):
            os.makedirs(path)
        return path
    except Exception as e:
        raise e


# creating all the necessary directories
_makedir(path)
cpath = _makedir(os.path.join(path, 'cpu_profile/'))
mpath = _makedir(os.path.join(path, 'memory_profiles/'))


def cpu_profile(func):
    def wrapper(*args, **kwargs):
        # every function will have its different profile
        datafn = cpath + func.__name__ + timestr + ".profile"

        # profiling the function by passing in the function and arguments
        prof = cProfile.Profile()
        retval = prof.runcall(func, *args, **kwargs)

        # dump the stats to a file
        prof.dump_stats(datafn)
        return retval
    return wrapper


def cpu_profiler(cls, profile_cpu=c['Profile_CPU']):
    if not profile_cpu:
        pass
    else:
        for key, value in vars(cls).iteritems():
            if callable(value):
                # adding cpu_profile decorator to every function of class
                setattr(cls, key, cpu_profile(value))
    return cls


def mem_profiler(cls, profile_memory=c['Profile_Memory']):
    if not profile_memory:
        pass
    else:
        file_name = mpath+cls.__name__+timestr+'.log'
        fp = open(file_name, 'w+')
        for key, value in vars(cls).iteritems():
            if callable(value):
                # adding profile decorator to every function of class
                setattr(cls, key, profile(value, stream=fp, precision=6))
    return cls
