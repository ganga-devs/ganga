"""LCG Athena meta-data utilities.

N.B. This code is under development and should not generally be used or relied upon.

"""

from Ganga.Lib.MonitoringServices.Dashboard import CommonUtil

#----- client meta-data builders -----
# TODO: add error handling code in following methods


def cl_application(job):
    """Build application. Only run on client."""
    return CommonUtil.strip_to_none(job.application.atlas_exetype)


def cl_application_version(job):
    """Build application_version. Only run on client."""
    if job.application.atlas_production:
        application_version = job.application.atlas_production
    else:
        application_version = job.application.atlas_release
    return CommonUtil.strip_to_none(application_version)


def cl_input_dataset(job):
    """Build input_dataset. Only run on client."""
    if not job.inputdata:
        return None
    datasetcsv = ','.join(job.inputdata.dataset)
    return CommonUtil.strip_to_none(datasetcsv)


def cl_jstoolui():
    """Build jstoolui. Only run on client."""
    return CommonUtil.hostname()


def cl_nevents_requested(job):
    """Build nevents_requested. Only run on client."""
    max_events = None
    if job.application.max_events > -1:
        max_events = job.application.max_events
    return max_events


def cl_output_dataset(job):
    """Build output_dataset. Only run on client."""
    if not job.outputdata:
        return None
    return CommonUtil.strip_to_none(job.outputdata.datasetname)


def cl_output_se(job):
    """Build output_se. Only run on client."""
    if not job.outputdata:
        return None
    # job.outputdata.location can be a string or a list
    if isinstance(job.outputdata.location, list):
        locations = []
        for l in job.outputdata.location:
            if l and l not in locations:
                locations.append(l)
        locationcsv = ','.join(locations)
    else:
        locationcsv = job.outputdata.location
    return CommonUtil.strip_to_none(locationcsv)


def cl_target(job):
    """Build target. Only run on client."""
    if hasattr(job.backend, 'CE'):
        targets = []
        if job.backend.CE:
            targets.append('CE_%s' % job.backend.CE)
        for site in job.backend.requirements.sites:
            if site:
                targets.append('SITE_%s' % site)
        targetcsv = ','.join(targets)
        return CommonUtil.strip_to_none(targetcsv)
    else:
        return CommonUtil.hostname()


def cl_task_type(config):
    """Build task_type. Only run on client."""
    return config['task_type']


#----- worker node meta-data builders -----
# TODO: add error handling code in following methods

def wn_load_athena_stats():
    """Load Athena stats. Only run on worker node.

    If the Athena stats.pickle file cannot be read then an empty dictionary is
    returned.
    """
    import cPickle as pickle
    try:
        f = open('stats.pickle', 'r')
        try:
            stats = pickle.load(f)
        finally:
            f.close()
    except:
        stats = {}
    return stats
