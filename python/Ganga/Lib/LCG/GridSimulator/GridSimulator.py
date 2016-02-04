from Ganga.Utility.Config import getConfig
from Ganga.Utility.logging import getLogger

from Ganga.Utility.GridShell import getShell

from Ganga.GPIDev.Credentials.ICredential import ICredential

logger = getLogger()

logger.critical('LCG Grid Simulator ENABLED')

##########################################################################
# GRID SIMULATOR
##########################################################################

config = getConfig("GridSimulator")

def sleep(val):
    import time
    time.sleep(get_number(val))


def failed(val):
    t = get_number(val)
    import random
    return random.random() < t


def get_number(val):
    import random
    if isinstance(val, str):
        t = eval(val, {'random': random})
    else:
        t = val
    if not type(t) in [type(1.0), type(1)]:
        # print 'problem with configuration option, invalid value: %s'%val
        logger.error(
            'problem with configuration option, invalid value: %s', val)
        return 0
    # print t
    return t

import os
import time

cmd = 'simulation'


class GridSimulator(object):

    '''Simulator of LCG interactions'''

    middleware = 'GLITE'

    credential = None

    def __init__(self, middleware='GLITE'):
        self.active = True
        self.middleware = middleware.upper()
        self.credential = ICredential()  # FIXME: or the real one
        #import Ganga.Core.FileWorkspace
        #basedir = Ganga.Core.FileWorkspace.gettop()
        #basedir = '/tmp'
        basedir = '.'
        self.gridmap_filename = '%s/lcg_simulator_gridmap' % basedir
        import shelve
        # map Grid job id into inputdir (where JDL file is)
        self.jobid_map = shelve.open(self.gridmap_filename, writeback=False)
        self.jobid_map.setdefault('_job_count', 0)

        # here we store the job finish times as seen by ganga
        self.finished_jobs_filename = '%s/lcg_simulator_finished_jobs' % basedir
        self.ganga_finish_time = shelve.open(
            self.finished_jobs_filename, writeback=False)

        self.shell = getShell('GLITE')

        logger.critical('Grid Simulator data files: %s %s',
                        self.gridmap_filename, self.finished_jobs_filename)

    def check_proxy(self):
        return True

    def submit(self, jdlpath, ce=None):
        '''This method is used for normal and native bulk submission supported by GLITE middleware.'''

        logger.debug(
            'job submit command: submit(jdlpath=%s,ce=%s)', jdlpath, ce)

        jdl = eval(file(jdlpath).read())

        subjob_ids = []
        if jdl['Type'] == 'collection':
            import re
            # we need to parse the Nodes attribute string here
            r = re.compile(r'.*NodeName = "(gsj_\d+)"; file="(\S*)"*')
            for line in jdl['Nodes'].splitlines()[1:-1]:
                m = r.match(line)
                if m:
                    nodename, sjdl_path = m.groups()
                subjob_ids.append(
                    self._submit(sjdl_path, ce, [], nodename=nodename))

        masterid = self._submit(jdlpath, ce, subjob_ids)

        return masterid

    def _params_filename(self, jobid):
        inputdir = os.path.realpath(self.jobid_map[jobid])
        return os.path.join(inputdir, 'params')

    def _submit(self, jdlpath, ce, subjob_ids, nodename=None):
        '''Submit a JDL file to LCG'''

        logger.debug(
            'job submit command: _submit(jdlpath=%s,ce=%s,subjob_ids=%s)', jdlpath, ce, subjob_ids)

        inputdir = os.path.dirname(os.path.realpath(jdlpath))

        def write():
            file(os.path.join(inputdir, 'params'), 'w').write(
                repr(runtime_params))

        runtime_params = {}
        runtime_params['submission_time_start'] = time.time()

        sleep(config['submit_time'])
        runtime_params['submission_time_stop'] = time.time()

        if failed(config['submit_failure_rate']):
            runtime_params['status'] = 'failed_to_submit'
            write()
            logger.warning('Job submission failed.')
            return

        jobid = self._make_new_id()

        self.jobid_map[jobid] = inputdir

        runtime_params['jobid'] = jobid
        runtime_params['status'] = 'submitted'
        runtime_params['should_fail'] = failed(config['job_failure_rate'])
        runtime_params['expected_job_id_resolve_time'] = get_number(
            config['job_id_resolved_time'])
        runtime_params['expected_finish_time'] = time.time(
        ) + get_number(config['job_finish_time'])
        runtime_params['subjob_ids'] = subjob_ids
        runtime_params['nodename'] = nodename
        write()
        return jobid

    def _make_new_id(self):
        self.jobid_map['_job_count'] += 1
        jobid = 'https://ganga.simulator.cern.ch/%d' % self.jobid_map[
            '_job_count']
        return jobid

    def _cancel(self, jobid):
        inputdir = self.jobid_map[jobid]

        sleep(config['cancel_time'])
        if failed(config['cancel_failure_rate']):
            file(self._params_filename(jobid), 'a').write(
                '\n failed to cancel: %d' % time.time())
            return False
        file(self._params_filename(jobid), 'a').write(
            '\ncancelled: %d' % time.time())
        return True

    def native_master_cancel(self, jobid):
        '''Native bulk cancellation supported by GLITE middleware.'''

        logger.debug(
            'job cancel command: native_master_cancel(jobid=%s', jobid)

        # FIXME: TODO: emulate bulk!
        return self._cancel(jobid)

    def _status(self, jobid, has_id):
        logger.debug(
            'job status command: _status(jobid=%s,has_id=%d)', jobid, has_id)

        info = {'id': None,
                'name': None,
                'status': None,
                'exit': None,
                'reason': None,
                'is_node': False,
                'destination': 'anywhere'}

        params = eval(file(self._params_filename(jobid)).read())

        sleep(config['single_status_time'])

        assert params['jobid'] == jobid

        if has_id:
            info['id'] = params['jobid']
            info['name'] = params['nodename']

        # if is_collection and time.time() > params['expected_job_id_resolve_time']:
        #    info['name'] = 'node_%d' % 0 # FIXME: some number (read from jdl?)

        logger.debug('current_time-expected_finish_time = %d',
                     time.time() - params['expected_finish_time'])

        if time.time() > params['expected_finish_time']:
            if params['should_fail']:
                info['status'] = 'Aborted'
                info['reason'] = 'for no reason'
                info['exit'] = -1
                self.ganga_finish_time[jobid] = time.time()
            else:
                info['status'] = 'Done (Success)'
                info['exit'] = 0
                info['reason'] = 'for a reason'

        logger.debug('_status (jobid=%s) -> %s', jobid, repr(info))

        # PENDING: handle other statuses: 'Running','Aborted','Cancelled','Done
        # (Exit Code !=0)','Cleared'
        return info

    def status(self, jobids, is_collection=False):
        '''Query the status of jobs on the grid.
        If is_collection is False then jobids is a list of non-split jobs or emulated bulk subjobs of a single master job.
        If is_collection is True then jobids is a list of master jobs which are natively bulk.
        '''

        logger.debug(
            'job status command: status(jobid=%s,is_collection=%d)', jobids, is_collection)

        info = []

        for id in jobids:
            if is_collection:
                # print 'master _status'
                sleep(config['master_status_time'])
                info.append(self._status(id, True))
                # print 'master _status done'
                params = eval(file(self._params_filename(id)).read())
                # print 'master params',params
                has_id = time.time() > params['expected_job_id_resolve_time']
                for sid in params['subjob_ids']:
                    info.append(self._status(sid, has_id))
                    info[-1]['is_node'] = True
            else:
                has_id = False
                info.append(self._status(id, True))

        return info

    def get_loginfo(self, jobid, directory, verbosity=1):
        '''Fetch the logging info of the given job and save the output in the jobs outputdir'''

        return ""

    def get_output(self, jobid, directory, wms_proxy=False):
        '''Retrieve the output of a job on the grid'''

        logger.debug(
            'job get output command: get_output(jobid=%s,directory=%s)', jobid, directory)
        sleep(config['get_output_time'])
        self.ganga_finish_time[jobid] = time.time()
        return (True, None)

    def cancel(self, jobid):
        '''Cancel a job'''
        logger.debug('job cancel command: cancel(jobid=%s)', jobid)

        return self._cancel(jobid)

    @staticmethod
    def expandjdl(items):
        '''Expand jdl items'''

        return repr(items)
