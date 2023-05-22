from GangaCore.GPIDev.Base.Proxy import getRuntimeGPIObject, stripProxy


def lazyLoadJobFQID(this_job):
    return lazyLoadJobObject(this_job, "fqid")


def lazyLoadJobStatus(this_job):
    return lazyLoadJobObject(this_job, "status", False)


def lazyLoadJobBackend(this_job):
    return lazyLoadJobObject(this_job, "backend")


def lazyLoadJobApplication(this_job):
    return lazyLoadJobObject(this_job, "application")


def lazyLoadJobObject(raw_job, this_attr, do_eval=True):
    # Returns an object which corresponds to an attribute from a Job,
    # or matches it's default equivalent without triggering a load from disk
    # i.e. lazy loading a Dirac backend will return a raw Dirac() object
    # and lazy loading the status will return the status string
    # These are all evaluated from the strings in the index file for the job.
    # dont_eval lets the method know a string is expected to be returned and
    # not evaluated so nothing is evaluated against the GPI

    this_job = stripProxy(raw_job)

    if this_job._getRegistry() is not None:
        if this_job._getRegistry().has_loaded(this_job):
            return getattr(this_job, this_attr)

    lzy_loading_str = "display:" + this_attr
    job_index_cache = this_job._index_cache
    if isinstance(job_index_cache, dict) and lzy_loading_str in job_index_cache:
        obj_name = job_index_cache[lzy_loading_str]
        if obj_name is not None and do_eval:
            job_obj = stripProxy(getRuntimeGPIObject(obj_name, True))
            if job_obj is None:
                job_obj = getattr(this_job, this_attr)
        elif not do_eval:
            job_obj = obj_name
        else:
            job_obj = getattr(this_job, this_attr)

    else:
        job_obj = getattr(this_job, this_attr)

    return job_obj
