from __future__ import print_function

import datetime
from collections import namedtuple

import os
import random
import tempfile
import time
from textwrap import dedent
import uuid

import pytest

from Ganga.Utility.logging import getLogger
from GangaDirac.Lib.Utilities.DiracUtilities import execute

from Ganga.testlib.mark import external
from Ganga.testlib.GangaUnitTest import load_config_files, clear_config

logger = getLogger(modulename=True)

statusmapping = {
    'Checking': 'submitted',
    'Completed': 'running',
    'Deleted': 'failed',
    'Done': 'completed',
    'Failed': 'failed',
    'Killed': 'killed',
    'Matched': 'submitted',
    'Received': 'submitted',
    'Running': 'running',
    'Staging': 'submitted',
    'Stalled': 'running',
    'Waiting': 'submitted',
}


JobInfo = namedtuple('JobInfo', ['id', 'get_file_lfn', 'remove_file_lfn'])


@pytest.yield_fixture(scope='module')
def load_config():
    """Load the Ganga config files before the test and clean them up afterwards"""
    load_config_files()
    yield
    clear_config()


@pytest.yield_fixture(scope='class')
def dirac_job(load_config):

    sandbox_str = uuid.uuid4()
    get_file_str = uuid.uuid4()
    remove_file_str = uuid.uuid4()

    exe_script = """
    #!/bin/bash
    echo '%s' > sandboxFile.txt
    echo '%s' > getFile.dst
    echo '%s' > removeFile.dst
    """ % (sandbox_str, get_file_str, remove_file_str)

    logger.info("exe_script:\n%s\n" % str(exe_script))

    exe_file, exe_path_name = tempfile.mkstemp()
    with os.fdopen(exe_file, 'wb') as f:
        f.write(exe_script)

    api_script = """
    from DIRAC.Interfaces.API.Dirac import Dirac
    from DIRAC.Interfaces.API.Job import Job
    from DIRAC.Core.Utilities.SiteSEMapping import getSEsForCountry

    uk_ses = getSEsForCountry('uk')['Value']

    j = Job()
    j.setName('Ganga-DiracCommands-InitTestJob')
    j.setCPUTime(10)
    j.setExecutable('###EXE_SCRIPT_BASE###','','Ganga_Executable.log')
    j.setInputSandbox(['###EXE_SCRIPT###'])
    j.setOutputSandbox(['std.out','std.err','sandboxFile.txt'])
    j.setOutputData(['getFile.dst', 'removeFile.dst'], outputSE=uk_ses)
    j.setBannedSites(['LCG.CERN.ch', 'LCG.CNAF.it', 'LCG.GRIDKA.de', 'LCG.IN2P3.fr', 'LCG.NIKHEF.nl', 'LCG.PIC.es', 'LCG.RAL.uk', 'LCG.SARA.nl'])
    #submit the job to dirac
    dirac=Dirac()
    result = dirac.submit(j)
    output(result)
    """
    api_script = dedent(api_script)

    final_submit_script = api_script.replace('###EXE_SCRIPT###', exe_path_name).replace('###EXE_SCRIPT_BASE###', os.path.basename(exe_path_name))
    confirm = execute(final_submit_script, return_raw_dict=True)
    print('Output from submit command', confirm)
    if not isinstance(confirm, dict):
        raise RuntimeError('Problem submitting job\n{0}'.format(confirm))

    assert 'OK' in confirm, 'Failed to submit job!'
    assert confirm['OK'], 'Failed to submit job!'
    job_id = confirm['Value']
    logger.info(job_id)

    os.remove(exe_path_name)

    logger.info('Waiting for DIRAC job to finish')
    timeout = 1200
    end_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout)
    status = execute('status([%s], %s)' % (job_id, repr(statusmapping)), return_raw_dict=True)
    print('Output from first status command', status)
    while (status['OK'] and statusmapping[status['Value'][0][1]] not in ['completed', 'failed'] )and datetime.datetime.utcnow() < end_time:
        time.sleep(5)
        status = execute('status([%s], %s)' % (job_id, repr(statusmapping)), return_raw_dict=True)
        print("Job status: %s" % status)

    assert 'OK' in status, 'Failed to get job Status!'
    assert status['OK'], 'Failed to get job Status!'
    assert statusmapping[status['Value'][0][1]] == 'completed', 'job not completed properly: %s' % status

    logger.info("status: %s", status)

    output_data_info = execute('getOutputDataInfo("%s")' % job_id, return_raw_dict=True)
    logger.info('output_data_info: %s' % output_data_info)
    max_retry = 20
    count = 0
    while not output_data_info.get('OK', True) and count != max_retry:
        time.sleep(5)
        output_data_info = execute('getOutputDataInfo("%s")' % job_id, return_raw_dict=True)
        logger.info("output_data_info:\n%s\n", output_data_info)
        count+=1
    
    assert 'OK' in output_data_info, 'getOutputDataInfo Failed!'
    assert output_data_info['OK'], 'getOutputDataInfo Failed!'

    logger.info("\n\n\noutput_data_info: %s\n\n\n" % output_data_info)
    get_file_lfn = output_data_info['Value']['getFile.dst']['LFN']
    remove_file_lfn = output_data_info['Value']['removeFile.dst']['LFN']
    logger.info("%s %s", get_file_lfn, remove_file_lfn)

    yield JobInfo(job_id, get_file_lfn, remove_file_lfn)

    confirm = execute('removeFile("%s")' % get_file_lfn, return_raw_dict=True)
    assert 'OK' in confirm, 'removeFile Failed!'
    assert confirm['OK'], 'removeFile Failed!'


@pytest.fixture(scope='module')
def dirac_sites(load_config):
    """Grab a shuffled list of UK DIRAC storage elements"""
    site_script = dedent("""
        from DIRAC.Core.Utilities.SiteSEMapping import getSEsForCountry
        output(getSEsForCountry('uk'))
        """)
    output = execute(site_script, return_raw_dict=True)
    assert output['OK'], 'Could not fetch list of SEs'
    sites = output['Value']
    random.shuffle(sites)
    return sites


@external
class TestDiracCommands(object):
    def test_peek(self, dirac_job):
        confirm = execute('peek("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'peek command not executed successfully'

    def test_getJobCPUTime(self, dirac_job):
        confirm = execute('getJobCPUTime("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'getJobCPUTime command not executed successfully'

    def test_getOutputData(self, dirac_job):
        confirm = execute('getOutputData("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'getOutputData command not executed successfully'

    def test_getOutputSandbox(self, dirac_job):
        confirm = execute('getOutputSandbox("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'getOutputSandbox command not executed successfully'

    def test_getOutputDataInfo(self, dirac_job):
        confirm = execute('getOutputDataInfo("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'getOutputDataInfo command not executed successfully'
        assert isinstance(confirm['Value']['getFile.dst'], dict), 'getOutputDataInfo command not executed successfully'

    def test_getOutputDataLFNs(self, dirac_job):
        confirm = execute('getOutputDataLFNs("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        logger.info(confirm)
        assert confirm['OK'], 'getOutputDataLFNs command not executed successfully'

    def test_normCPUTime(self, dirac_job):
        confirm = execute('normCPUTime("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'normCPUTime command not executed successfully'
        assert isinstance(confirm['Value'], str), 'normCPUTime ommand not executed successfully'

    def test_getStateTime(self, dirac_job):
        confirm = execute('getStateTime("%s", "completed")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'getStateTime command not executed successfully'
        assert isinstance(confirm['Value'], datetime.datetime), 'getStateTime command not executed successfully'

    def test_timedetails(self, dirac_job):
        confirm = execute('timedetails("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'timedetails command not executed successfully'
        assert isinstance(confirm['Value'], dict), 'Command not executed successfully'

    def test_y_reschedule(self, dirac_job):
        confirm = execute('reschedule("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'reschedule command not executed successfully'

    def test_z_kill(self, dirac_job):
        # remove_files()
        confirm = execute('kill("%s")' % dirac_job.id, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'kill command not executed successfully'

    def test_status(self, dirac_job):
        confirm = execute('status([%s], %s)' % (dirac_job.id, repr(statusmapping)), return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'status command not executed successfully'
        assert isinstance(confirm['Value'], list), 'Command not executed successfully'

    def test_getFile(self, dirac_job):
        confirm = execute('getFile("%s")' % dirac_job.get_file_lfn, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'getFile command not executed successfully'

    def test_removeFile(self, dirac_job):
        confirm = execute('removeFile("%s")' % dirac_job.remove_file_lfn, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'removeFile command not executed successfully'

    def test_ping(self, dirac_job):
        confirm = execute('ping("WorkloadManagement","JobManager")', return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'ping command not executed successfully'

    def test_getMetadata(self, dirac_job):
        confirm = execute('getMetadata("%s")' % dirac_job.get_file_lfn, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'getMetaData command not executed successfully'

    def test_getReplicas(self, dirac_job):
        confirm = execute('getReplicas("%s")' % dirac_job.get_file_lfn, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'getReplicas command not executed successfully'

    def test_replicateFile(self, dirac_job, dirac_sites):

        for new_location in dirac_sites:
            confirm = execute('replicateFile("%s","%s","")' % (dirac_job.get_file_lfn, new_location), return_raw_dict=True)
            logger.info(confirm)
            if not confirm['OK']:
                continue  # If we couldn't add the file, try the next site
            confirm = execute('removeReplica("%s","%s")' % (dirac_job.get_file_lfn, new_location), return_raw_dict=True)
            logger.info(confirm)
            assert confirm['OK'], 'Command not executed successfully'
            break  # Once we found a working site, stop looking
        else:
            raise AssertionError('No working site found')

    def test_splitInputData(self, dirac_job):
        confirm = execute('splitInputData("%s","1")' % dirac_job.get_file_lfn, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'splitInputData command not executed successfully'

    def test_uploadFile(self, tmpdir, dirac_job, dirac_sites):

        new_lfn = '%s_add_file' % os.path.dirname(dirac_job.get_file_lfn)

        for location in dirac_sites:
            temp_file = tmpdir.join('upload_file')
            temp_file.write(uuid.uuid4())
            logger.info('Adding file to %s', location)
            confirm = execute('uploadFile("%s","%s",["%s"],"")' % (new_lfn, temp_file, location), return_raw_dict=True)
            logger.info(confirm)
            if confirm.get(location, False):
                continue  # If we couldn't add the file, try the next site
            logger.info('Removing file from %s', location)
            confirm_remove = execute('removeFile("%s")' % new_lfn, return_raw_dict=True)
            logger.info(confirm)
            assert confirm_remove['OK'], 'Command not executed successfully'
            break  # Once we found a working site, stop looking
        else:
            raise AssertionError('No working site found')

    def test_addFile(self, tmpdir, dirac_job, dirac_sites):

        new_lfn = '%s_add_file' % os.path.dirname(dirac_job.get_file_lfn)

        for location in dirac_sites:
            temp_file = tmpdir.join('add_file')
            temp_file.write(uuid.uuid4())
            logger.info('Adding file to %s', location)
            confirm = execute('addFile("%s","%s","%s","")' % (new_lfn, temp_file, location), return_raw_dict=True)
            logger.info(confirm)
            if not confirm['OK']:
                continue  # If we couldn't add the file, try the next site
            logger.info('Removing file from %s', location)
            confirm_remove = execute('removeFile("%s")' % new_lfn, return_raw_dict=True)
            logger.info(confirm)
            assert confirm_remove['OK'], 'Command not executed successfully'
            break  # Once we found a working site, stop looking
        else:
            raise AssertionError('No working site found')

    def test_getJobGroupJobs(self, dirac_job):
        confirm = execute('getJobGroupJobs("")', return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

