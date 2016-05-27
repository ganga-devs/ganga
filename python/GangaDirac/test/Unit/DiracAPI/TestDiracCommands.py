from __future__ import print_function

import datetime
from collections import namedtuple

import os
import random
import tempfile
import time
from textwrap import dedent

import pytest

from Ganga.Utility.logging import getLogger
from GangaDirac.Lib.Utilities.DiracUtilities import execute

from Ganga.testlib.mark import external
from Ganga.testlib.GangaUnitTest import load_config_files, clear_config

logger = getLogger(modulename=True)

random.seed()

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


def random_str():

    t = datetime.datetime.now()
    unix_t = time.mktime(t.timetuple())

    file_string = str(unix_t) + " "

    rand_num = random.random() * 1E10

    file_string += str(rand_num)

    return file_string


JobInfo = namedtuple('JobInfo', ['id', 'get_file_lfn', 'remove_file_lfn'])


@pytest.yield_fixture(scope='class')
def dirac_job():
    load_config_files()

    sandbox_str = random_str()
    time.sleep(0.5)
    get_file_str = random_str()
    time.sleep(0.5)
    remove_file_str = random_str()

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
    confirm = execute(final_submit_script)
    if not isinstance(confirm, dict):
        raise RuntimeError('Problem submitting job\n{0}'.format(confirm))

    job_id = confirm['Value']
    logger.info(job_id)

    os.remove(exe_path_name)

    logger.info('Waiting for DIRAC job to finish')
    timeout = 1200
    end_time = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout)
    status = execute('status([%s], %s)' % (job_id, repr(statusmapping)))
    while statusmapping[status[0][1]] not in ['completed', 'failed'] and datetime.datetime.utcnow() < end_time:
        time.sleep(5)
        status = execute('status([%s], %s)' % (job_id, repr(statusmapping)))

    assert statusmapping[status[0][1]] == 'completed', 'job not completed properly: %s' % status

    logger.info("status: %s", status)

    output_data_info = execute('getOutputDataInfo("%s")' % job_id)
    logger.info(output_data_info)
    while not output_data_info.get('OK', True):
        time.sleep(5)
        output_data_info = execute('getOutputDataInfo("%s")' % job_id)
        logger.info("\n%s\n", output_data_info)

    logger.info("\n\n\noutput_data_info: %s\n\n\n" % output_data_info)
    get_file_lfn = output_data_info['getFile.dst']['LFN']
    remove_file_lfn = output_data_info['removeFile.dst']['LFN']
    logger.info("%s %s", get_file_lfn, remove_file_lfn)

    yield JobInfo(job_id, get_file_lfn, remove_file_lfn)

    confirm = execute('removeFile("%s")' % get_file_lfn)
    assert confirm['OK'], 'Command not executed successfully'
    clear_config()


@external
class TestDiracCommands(object):
    def test_peek(self, dirac_job):
        confirm = execute('peek("%s")' % dirac_job.id)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_getJobCPUTime(self, dirac_job):
        confirm = execute('getJobCPUTime("%s")' % dirac_job.id)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_getOutputData(self, dirac_job):
        confirm = execute('getOutputData("%s")' % dirac_job.id)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_getOutputSandbox(self, dirac_job):
        confirm = execute('getOutputSandbox("%s")' % dirac_job.id)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_getOutputDataInfo(self, dirac_job):
        confirm = execute('getOutputDataInfo("%s")' % dirac_job.id)
        logger.info(confirm)
        assert isinstance(confirm['getFile.dst'], dict), 'Command not executed successfully'

    def test_getOutputDataLFNs(self, dirac_job):
        confirm = execute('getOutputDataLFNs("%s")' % dirac_job.id)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_normCPUTime(self, dirac_job):
        confirm = execute('normCPUTime("%s")' % dirac_job.id)
        assert isinstance(confirm, str), 'Command not executed successfully'

    def test_getStateTime(self, dirac_job):
        confirm = execute('getStateTime("%s", "completed")' % dirac_job.id)
        logger.info(confirm)
        assert isinstance(confirm, datetime.datetime), 'Command not executed successfully'

    def test_timedetails(self, dirac_job):
        confirm = execute('timedetails("%s")' % dirac_job.id)
        logger.info(confirm)
        assert isinstance(confirm, dict), 'Command not executed successfully'

    def test_y_reschedule(self, dirac_job):
        confirm = execute('reschedule("%s")' % dirac_job.id)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_z_kill(self, dirac_job):
        # remove_files()
        confirm = execute('kill("%s")' % dirac_job.id)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_status(self, dirac_job):
        confirm = execute('status([%s], %s)' % (dirac_job.id, repr(statusmapping)))
        logger.info(confirm)
        assert isinstance(confirm, list), 'Command not executed successfully'

    def test_getFile(self, dirac_job):
        confirm = execute('getFile("%s")' % dirac_job.get_file_lfn)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_removeFile(self, dirac_job):
        confirm = execute('removeFile("%s")' % dirac_job.remove_file_lfn)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_ping(self, dirac_job):
        confirm = execute('ping("WorkloadManagement","JobManager")')
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_getMetadata(self, dirac_job):
        confirm = execute('getMetadata("%s")' % dirac_job.get_file_lfn)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_getReplicas(self, dirac_job):
        confirm = execute('getReplicas("%s")' % dirac_job.get_file_lfn)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_a_replicateFile(self, dirac_job):
        new_location = 'UKI-SOUTHGRID-OX-HEP-disk'
        confirm = execute('replicateFile("%s","%s","")' % (dirac_job.get_file_lfn, new_location))
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_b_removeReplica(self, dirac_job):
        new_location = 'UKI-SOUTHGRID-OX-HEP-disk'
        confirm = execute('removeReplica("%s","%s")' % (dirac_job.get_file_lfn, new_location))
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_splitInputData(self, dirac_job):
        confirm = execute('splitInputData("%s","1")' % dirac_job.get_file_lfn)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_uploadFile(self, dirac_job):
        new_lfn = '%s_upload_file' % os.path.dirname(dirac_job.get_file_lfn)
        location = 'UKI-SOUTHGRID-RALPP-disk'

        add_file = open('upload_file', 'w')
        add_file.write(random_str())
        add_file.close()

        confirm = execute('uploadFile("%s","upload_file","%s")' % (new_lfn, location))
        assert isinstance(confirm, dict), 'Command not executed successfully'
        confirm_remove = execute('removeFile("%s")' % new_lfn)
        assert confirm_remove['OK'], 'Command not executed successfully'

    def test_addFile(self, dirac_job):
        new_lfn = '%s_add_file' % os.path.dirname(dirac_job.get_file_lfn)
        location = 'UKI-SOUTHGRID-RALPP-disk'
        add_file = open('add_file', 'w')
        add_file.write(random_str())
        add_file.close()
        confirm = execute('addFile("%s","add_file","%s","")' % (new_lfn, location))
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'
        confirm_remove = execute('removeFile("%s")' % new_lfn)
        logger.info(confirm)

        assert confirm_remove['OK'], 'Command not executed successfully'

    def test_getJobGroupJobs(self, dirac_job):
        confirm = execute('getJobGroupJobs("")')
        assert confirm['OK'], 'Command not executed successfully'

# LHCb commands:

    @pytest.mark.skip(reason='Should be moved to LHCb')
    def test_bkQueryDict(self, dirac_job):
        confirm = execute('bkQueryDict({"FileType":"Path","ConfigName":"LHCb","ConfigVersion":"Collision09","EventType":"10","ProcessingPass":"Real Data","DataTakingConditions":"Beam450GeV-VeloOpen-MagDown"})')
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    @pytest.mark.skip(reason='Should be moved to LHCb')
    def test_checkSites(self, dirac_job):
        confirm = execute('checkSites()')
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    @pytest.mark.skip(reason='Should be moved to LHCb')
    def test_bkMetaData(self, dirac_job):
        confirm = execute('bkMetaData("")')
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    @pytest.mark.skip(reason='Should be moved to LHCb')
    def test_getDataset(self, dirac_job):
        confirm = execute('getDataset("LHCb/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + RecoToDST-07/10/DST","","Path","","","")')
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    @pytest.mark.skip(reason='Should be moved to LHCb')
    def test_checkTier1s(self, dirac_job):
        confirm = execute('checkTier1s()')
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

# Problematic tests

    @pytest.mark.skip(reason='Should be moved to LHCb')
    def test_getInputDataCatalog(self, dirac_job):
        confirm = execute('getInputDataCatalog("%s","","")' % dirac_job.get_file_lfn)
        logger.info(confirm)
        assert confirm['Message'] == 'Failed to access all of requested input data', 'Command not executed successfully'

    @pytest.mark.skip(reason='Should be moved to LHCb')
    def test_getLHCbInputDataCatalog(self, dirac_job):
        confirm = execute('getLHCbInputDataCatalog("%s",0,"","")' % dirac_job.get_file_lfn)
        logger.info(confirm)
        assert confirm['Message'] == 'Failed to access all of requested input data', 'Command not executed successfully'
