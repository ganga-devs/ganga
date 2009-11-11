"""Dashboard LCG/Athena Monitoring Service plugin

N.B. This code is under development and should not generally be used or relied upon.

"""

#TODO: disable if backend/application combination is not LCG/Athena
#TODO: report LB state changes: e.g. in complete method and perhaps using new hooks.
#TODO: add task_meta_message, job_meta_message, job_processing_attributes_message

from Ganga.Lib.MonitoringServices.Dashboard.DashboardMS import DashboardMS
class LCGAthenaMS(DashboardMS):
    """Dashboard LCG/Athena Monitoring Service based on MSG."""
    
    def __init__(self, job_info, config_info):
        """Construct the Dashboard Athena Monitoring Service."""
        DashboardMS.__init__(self, job_info, config_info)

    def getSandboxModules(self):
        """Return list of DashboardMS module dependencies."""
        import Ganga.Lib.MonitoringServices.Dashboard
        return DashboardMS.getSandboxModules(self) + [
            Ganga.Lib.MonitoringServices.Dashboard.LCGAthenaMS,
            ]

    def getJobInfo(self): # called on client, so job_info is Job object
        """Create job_info from Job object."""
        job = self.job_info
        ji = {
            'id': job.id,
            'EXECUTION_BACKEND': _cl_execution_backend(job),
            'GRID_CERTIFICATE': _cl_grid_certificate(),
            'TASKNAME': _cl_task_name(job),
            'JOB_ID_INSIDE_THE_TASK': _cl_job_id_inside_the_task(job),
            'UNIQUEJOBID': _cl_unique_job_id(job),
            }
        return ji
    
    #----- event call-backs -----

    def submitting(self,**opts): # called on client, so job_info is Job object
        self._log('debug', 'submitting %s' % self.job_info.id)

    def prepare(self,**opts): # called on client, so job_info is Job object
        self._log('debug', 'prepare %s' % self.job_info.id)

    def submit(self, **opts): # called on client, so job_info is Job object
        """Log submit event on client."""
        self._log('debug', 'submit %s' % self.job_info.id)
        message = self._cl_job_status_message('submitted', _utcnow())
        self._send(message)

    def start(self, **opts): # called on worker node, so job_info is dictionary
        """Log start event on worker node."""
        self._log('debug', 'start %s' % self.job_info['id'])
        message = self._wn_job_status_message('running', _utcnow())
        self._send(message)

    def progress(self, **opts): # called on worker node, so job_info is dictionary
        self._log('debug', 'progress %s' % self.job_info['id'])

    def stop(self, exitcode, **opts): # called on worker node, so job_info is dictionary
        """Log stop event on worker node."""
        self._log('debug', 'stop %s' % self.job_info['id'])
        if exitcode == 0:
            status = 'completed'
        else:
            status = 'failed'
        message = self._wn_job_status_message(status, _utcnow())
        message['JOBEXITCODE'] = exitcode
        self._send(message)

    def complete(self,**opts): # called on client, so job_info is Job object
        """Log complete event on client."""
        self._log('debug', 'complete %s' % self.job_info.id)
        if opts['cause'] == 'failed':
            status = 'failed' #TODO it could be killed. how could we know?
        else:
            status = 'completed'
        # we do not specify state_start_time since we do not know it
        message = self._cl_job_status_message(status, None)
        self._send(message)

    def rollback(self,**opts): # called on client, so job_info is Job object
        self._log('debug', 'rollback %s' % self.job_info.id)

    #----- message builders -----

    def _cl_job_status_message(self, status, status_start_time=None):
        # Not null: EXECUTION_BACKEND, GRIDJOBID, JOB_ID_INSIDE_THE_TASK, TASKNAME, UNIQUEJOBID
        job = self.job_info # on client, so job_info is Job object
        msg = {
            'DESTCE': _cl_dest_ce(job),
            'DESTSITE': None,
            'DESTWN': None,
            'EXECUTION_BACKEND': _cl_execution_backend(job),
            'GRIDEXITCODE': _cl_grid_exit_code(job),
            'GRIDEXITREASON': _cl_grid_exit_reason(job),
            'GRIDJOBID': _cl_grid_job_id(job),
            'GRID_CERTIFICATE': _cl_grid_certificate(),
            'JOBEXITCODE': _cl_job_exit_code(job),
            'JOBEXITREASON': None,
            'JOB_ID_INSIDE_THE_TASK': _cl_job_id_inside_the_task(job),
            'REPORTER': 'ToolUI',
            'REPORTTIME': _utcnow(),
            'STATENAME': status,
            'STATESOURCE': 'Ganga',
            'STATESTARTTIME': status_start_time,
            'TASKNAME': _cl_task_name(job),
            'UNIQUEJOBID': _cl_unique_job_id(job),
            }
        return msg

    def _wn_job_status_message(self, status, status_start_time):
        # Not null: EXECUTION_BACKEND, GRIDJOBID, JOB_ID_INSIDE_THE_TASK, TASKNAME, UNIQUEJOBID
        ji = self.job_info # on worker node, so job_info is dictionary
        msg = {
            'DESTCE': _wn_dest_ce(),
            'DESTSITE': _wn_dest_site(),
            'DESTWN': _wn_dest_wn(),
            'EXECUTION_BACKEND': ji['EXECUTION_BACKEND'],
            'GRIDEXITCODE': None,
            'GRIDEXITREASON': None,
            'GRIDJOBID': _wn_grid_job_id(),
            'GRID_CERTIFICATE': ji['GRID_CERTIFICATE'],
            'JOBEXITCODE': None,
            'JOBEXITREASON': None,
            'JOB_ID_INSIDE_THE_TASK': ji['JOB_ID_INSIDE_THE_TASK'],
            'REPORTER': 'JobWN',
            'REPORTTIME': _utcnow(),
            'STATENAME': status,
            'STATESOURCE': 'Ganga',
            'STATESTARTTIME': status_start_time,
            'TASKNAME': ji['TASKNAME'],
            'UNIQUEJOBID': ji['UNIQUEJOBID'],
            }
        return msg


#----- utility methods -----

def _env(key):
    """Return the environment variable value corresponding to key.
    
    If the variable is undefined or empty then None is returned.
    """
    import os
    try:
        value = os.environ[key]
    except KeyError:
        value = None
    return _strip_to_none(value)
    
# utility method copied from Ganga.Utility.util
def _hostname():
    """ Try to get the hostname in the most possible reliable way as described in the Python LibRef."""
    import socket
    try:
        return socket.gethostbyaddr(socket.gethostname())[0]
    # [bugfix #20333]: 
    # while working offline and with an improper /etc/hosts configuration       
    # the localhost cannot be resolved 
    except:
        return 'localhost'

def _stdout(command):
    """Execute the command in a subprocess and return stdout.
    
    If the exit code is non-zero then None is returned.
    """
    import popen2
    p = popen2.Popen3(command)
    rc = p.wait()
    if rc == 0:
        return p.fromchild.read()
    else:
        return None

def _strip_to_none(value):
    """Returns the stripped string representation of value, or None if this is
    None or an empty string."""
    if value is None:
        return None
    text = str(value).strip()
    if len(text) == 0:
        return None
    return text

def _utcnow():
    """Return a UTC datetime with no timezone specified."""
    import datetime
    return datetime.datetime.utcnow()
    

#----- client meta-data builders ----- 
#TODO: add error handling code in following methods

def _cl_dest_ce(job):
    """Build dest_ce. Only run on client."""
    return _strip_to_none(job.backend.actualCE)

def _cl_execution_backend(job):
    """Build execution_backend. Only run on client."""
    return job.backend._name

def _cl_grid_exit_code(job):
    """Build grid_exit_code. Only run on client."""
    return _strip_to_none(job.backend.exitcode_lcg)

def _cl_grid_exit_reason(job):
    """Build grid_exit_reason. Only run on client."""
    return _strip_to_none(job.backend.reason)

def _cl_grid_job_id(job):
    """Build grid_job_id. Only run on client."""
    return _strip_to_none(job.backend.id)

def _cl_grid_certificate():
    """Build grid_certificate. Only run on client."""
    from Ganga.GPIDev import Credentials
    proxy = Credentials.getCredential('GridProxy')
    grid_certificate = proxy.info('-subject')
    return _strip_to_none(grid_certificate)

def _cl_job_id_inside_the_task(job):
    """Build job_id_inside_the_task. Only run on client."""
    if job.master is None:
        job_id_inside_the_task = 0
    else:
        job_id_inside_the_task = job.id
    return job_id_inside_the_task

def _cl_job_exit_code(job):
    """Build job_exit_code. Only run on client."""
    return _strip_to_none(job.backend.exitcode)

def _cl_task_name(job):
    """Build task_name. Only run on client."""
    from Ganga.Utility import Config
    user = Config.getConfig('Configuration')['user']
    rep_type = Config.getConfig('Configuration')['repositorytype']
    if 'Local' in rep_type:
        from Ganga.Runtime import Repository_runtime
        rep_dir = Repository_runtime.getLocalRoot()
        rep_hostname = Config.getConfig('System')['GANGA_HOSTNAME']
        repository = rep_hostname + ':' + rep_dir
    elif 'Remote' in rep_type:
        rep_host = Config.getConfig(rep_type +'_Repository')['host']
        rep_port = Config.getConfig(rep_type +'_Repository')['port']
        repository = rep_host + ':' + rep_port
    else:
        repository = ''

    if job.master is None:
        task_id = job.id
    else:
        task_id = job.master.id
    task_name = 'ganga_%s_%s@%s' %(task_id, user, repository)
    return task_name

def _cl_unique_job_id(job):
    """Build unique_job_id. Only run on client."""
    return job.info.uuid

    
#----- worker node meta-data builders ----- 
#TODO: add error handling code in following methods

def _wn_dest_ce():
    """Build dest_ce. Only run on worker node."""
    dest_ce = _env('GLOBUS_CE')
    if not dest_ce:
        dest_ce = _stdout('edg-brokerinfo getCE')
    if not dest_ce:
        dest_ce = _stdout('glite-brokerinfo getCE')
    return _strip_to_none(dest_ce)

def _wn_dest_site():
    """Build dest_site. Only run on worker node."""
    return _env('SITE_NAME')
    
def _wn_dest_wn():
    """Build dest_wn. Only run on worker node."""
    return _hostname()

def _wn_grid_job_id():
    """Build grid_job_id. Only run on worker node."""
    grid_job_id = _env('EDG_WL_JOBID')
    if not grid_job_id:
        grid_job_id = _env('GLITE_WMS_JOBID')
    return _strip_to_none(grid_job_id)
