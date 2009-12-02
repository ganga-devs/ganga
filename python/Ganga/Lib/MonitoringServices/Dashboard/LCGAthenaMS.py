"""Dashboard LCG/Athena Monitoring Service plugin

N.B. This code is under development and should not generally be used or relied upon.

"""

#TODO: disable if backend is not LCG/Athena


from Ganga.Lib.MonitoringServices.Dashboard import CommonUtil
from Ganga.Lib.MonitoringServices.Dashboard import LCGUtil


from Ganga.Lib.MonitoringServices.Dashboard.LCGMS import LCGMS
class LCGAthenaMS(LCGMS):
    """Dashboard LCG/Athena Monitoring Service based on MSG."""
    
    def __init__(self, job_info, config_info):
        """Construct the Dashboard LCG/Athena Monitoring Service."""
        LCGMS.__init__(self, job_info, config_info)

    def getSandboxModules(self):
        """Return list of module dependencies."""
        import Ganga.Lib.MonitoringServices.Dashboard
        return LCGMS.getSandboxModules(self) + [
            Ganga.Lib.MonitoringServices.Dashboard.LCGAthenaMS,
            ]

    #----- event call-backs -----

    def submit(self, **opts):
        """Log submit event on client."""
        LCGMS.submit(self, **opts)
        j = self.job_info # called on client, so job_info is Job object
        # send task-meta message if this is a master or single job
        if j.master is None:
            message = self._cl_task_meta_message()
            self._send(self.config_info['destination_task_meta'], message)
        # send job-meta message if this is a sub or single job
        if not j.subjobs:
            message = self._cl_job_meta_message()
            if message['GRIDJOBID'] is None:
                # This is to handle the temporary workaround in
                # LCG.master_bulk_updateMonitoringInformation() which results in two
                # submit messages being sent, one without a grid_job_id.
                self._log('debug', 'Not sending redundant message on submit without grid_job_id for job %s.' % j.fqid)
            else:
                self._send(self.config_info['destination_job_meta'], message)
        

    def stop(self, exitcode, **opts):
        """Log stop event on client."""
        LCGMS.stop(self, exitcode, **opts)
        # send job-processing-attributes message if job successful
        if exitcode == 0:
            message = self._wn_job_processing_attributes_message()
            self._send(self.config_info['destination_job_processing_attributes'], message)
        
    def complete(self, **opts):
        """Log complete event on client."""
        LCGMS.complete(self, **opts)
        j = self.job_info # called on client, so job_info is Job object
        # send task-meta message if this is a master or single job to include data unknown at submission
        if j.master is None:
            message = self._cl_task_meta_message()
            self._send(self.config_info['destination_task_meta'], message)

    #----- message builders -----

    def _cl_task_meta_message(self):
        j = self.job_info # called on client, so job_info is Job object
        msg = {
            'APPLICATION': cl_application(j), # e.g. ATHENA
            'APPLICATIONVERSION': cl_application_version(j), # e.g. 15.5.1
            'INPUTDATASET': cl_input_dataset(j), # e.g. fdr08_run2.0052283.physics_Muon.merge.AOD.o3_f8_m10
            'JSTOOL': 'Ganga', # e.g. Ganga, Panda
            'JSTOOLUI': cl_jstoolui(), # hostname of client. e.g. lxplus246.cern.ch
            'OUTPUTDATASET': cl_output_dataset(j),# Unknown at submission. e.g. user09.DavidTuckett.ganga.420.20091125.FZK-LCG2_SCRATCHDISK
            'OUTPUTSE': cl_output_se(j), # Unknown at submission. e.g. FZK-LCG2_SCRATCHDISK
            'OWNERDN': LCGUtil.cl_ownerdn(), # Grid certificate. e.g. /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=dtuckett/CN=671431/CN=David Tuckett/CN=proxy
            'REPORTER': 'ToolUI', # e.g. ToolUI, JobWN
            'REPORTTIME': CommonUtil.utcnow(), # e.g. 2009-11-25T14:59:24.754249Z
            'SUBMISSIONTYPE': 'direct',
            'TARGET': cl_target(j), # e.g. CE_xxx,SITE_CSCS-LCG2_DATADISK,SITE_DESY-ZN_DATADISK
            'TASKNAME': LCGUtil.cl_task_name(j), # e.g. ganga_420_dtuckett@lxplus246.cern.ch:/afs/cern.ch/user/d/dtuckett/gangadir/repository/dtuckett/LocalAMGA
            'TASKTYPE': cl_task_type(self.config_info), # e.g. analysis, production, hammercloud etc.
            '___fqid' : j.fqid,
            }
        return msg

    def _cl_job_meta_message(self):
        j = self.job_info # called on client, so job_info is Job object
        msg = {
            'GRIDJOBID': LCGUtil.cl_grid_job_id(j), # e.g. https://grid-lb0.desy.de:9000/moqY5njFGurEuoDkkJmtBA
            'JOB_ID_INSIDE_THE_TASK': LCGUtil.cl_job_id_inside_the_task(j), # subjob id e.g. 0
            'NEVENTSREQUESTED': cl_nevents_requested(j), # None or non-negative number e.g. 100
            'PILOT': 0, # 0 = not pilot, 1 = pilot
            'PILOTNAME': None,
            'REPORTER': 'ToolUI', # e.g. ToolUI, JobWN
            'REPORTTIME': CommonUtil.utcnow(), # e.g. 2009-11-25T14:59:24.754249Z
            'TASKNAME': LCGUtil.cl_task_name(j), # e.g. ganga_420_dtuckett@lxplus246.cern.ch:/afs/cern.ch/user/d/dtuckett/gangadir/repository/dtuckett/LocalAMGA
            'UNIQUEJOBID': LCGUtil.cl_unique_job_id(j), # Ganga uuid e.g. 1c08ff3b-904f-4f77-a481-d6fa765813cb
            '___fqid' : j.fqid,
            }
        return msg

    def _wn_job_processing_attributes_message(self):
        ji = self.job_info # called on worker node, so job_info is dictionary
        athena_stats = wn_load_athena_stats()
        msg = {
            'GRIDJOBID': LCGUtil.wn_grid_job_id(), # e.g. https://grid-lb0.desy.de:9000/moqY5njFGurEuoDkkJmtBA
            'JOB_ID_INSIDE_THE_TASK': ji['JOB_ID_INSIDE_THE_TASK'], # subjob id e.g. 0
            'NEVENTSPROCESSED': athena_stats.get('totalevents'), # number of events processed. e.g. 100
            'NFILESPROCESSED': athena_stats.get('numfiles'), # number of files processed. e.g. 2
            'REPORTER': 'JobWN', # e.g. ToolUI, JobWN
            'REPORTTIME': CommonUtil.utcnow(), # e.g. 2009-11-25T14:59:24.754249Z
            'SYSTEMTIME': athena_stats.get('systemtime'), # system cpu time in seconds. e.g. 38.45 
            'TASKNAME': ji['TASKNAME'], # e.g. ganga_420_dtuckett@lxplus246.cern.ch:/afs/cern.ch/user/d/dtuckett/gangadir/repository/dtuckett/LocalAMGA
            'UNIQUEJOBID': ji['UNIQUEJOBID'], # Ganga uuid e.g. 1c08ff3b-904f-4f77-a481-d6fa765813cb
            'USERTIME': athena_stats.get('usertime'), # user cpu time in seconds. e.g. 479.0
            'WALLCLOCK': athena_stats.get('wallclock'), # wallclock time in seconds. e.g. 1040
            '___fqid' : ji['fqid'],
            }
        return msg


#----- client meta-data builders ----- 
#TODO: add error handling code in following methods
#TODO: move to appropriate XxxUtil modules

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
    return CommonUtil.strip_to_none(job.outputdata.datasetname)

def cl_output_se(job):
    """Build output_se. Only run on client."""
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
    ces = []
    sites = []
    targets = []
    for j in [job] + job.subjobs:
        ce = j.backend.CE
        if ce and ce not in ces:
            ces.append(ce)
            targets.append('CE_%s' % ce)
        for site in j.backend.requirements.sites:
            if site and site not in sites:
                sites.append(site)
                targets.append('SITE_%s' % site)
    targetcsv = ','.join(targets)
    return CommonUtil.strip_to_none(targetcsv)

def cl_task_type(config):
    """Build task_type. Only run on client."""
    return config['task_type']

#----- worker node meta-data builders ----- 

def wn_load_athena_stats():
    """Load Athena stats. Only run on worker node.
    
    If the Athena stats.pickle file cannot be read then an empty dictionary is
    returned.
    """
    import cPickle as pickle
    try:
        f = open('stats.pickle','r')
        try:
            stats = pickle.load(f)
        finally:
            f.close()
    except:
        stats = {}
    return stats
    
    