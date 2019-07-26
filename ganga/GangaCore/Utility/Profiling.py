from GangaCore.Utility.Config import getConfig

import os
import cProfile
import time
import json

# creating a timestamp
timestr = time.strftime("-%Y%m%d-%H%M%S")

# Obtaining the config from .gangarc file
c = getConfig('Configuration')

if c['Profile_Memory']:
    from memory_profiler import profile
    
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
ccpath = _makedir(os.path.join(path, 'call_counters/'))


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
        for key, value in vars(cls).items():
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
        for key, value in vars(cls).items():
            if callable(value):
                # adding profile decorator to every function of class
                setattr(cls, key, profile(value, stream=fp, precision=6))
    return cls


function_calls = {}


def call_counts(func):
    def wrapper(*args, **kwargs):
        wrapper.calls += 1
        function_calls[(wrapper.__name__)] = wrapper.calls
        # storing the call counter for each function
        # Need to find a more effiecient way to store
        with open(ccpath+'call_counter_logs'+timestr+'.json', 'w+') as fp:
            json.dump(function_calls, fp, indent=4)
        return func(*args, **kwargs)
    wrapper.__name__ = func.__name__
    wrapper.calls = 0
    return wrapper


def call_counter(cls, count_calls=c['Count_Calls']):
    if not count_calls:
        pass
    else:
        for key, value in vars(cls).items():
            if callable(value):
                # adding call_counts decorator to every function of class
                setattr(cls, key, call_counts(value))
    return cls
