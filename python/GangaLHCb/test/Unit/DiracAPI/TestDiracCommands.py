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


@pytest.yield_fixture(scope='module')
def load_config():
    """Load the Ganga config files before the test and clean them up afterwards"""
    load_config_files()
    yield
    clear_config()


@pytest.yield_fixture(scope='class')
def dirac_job(load_config):

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
    confirm = execute(final_submit_script, return_raw_dict=True)
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

    def test_bkQueryDict(self, dirac_job):
        confirm = execute('bkQueryDict({"FileType":"Path","ConfigName":"LHCb","ConfigVersion":"Collision09","EventType":"10","ProcessingPass":"Real Data","DataTakingConditions":"Beam450GeV-VeloOpen-MagDown"})', return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'bkQuery command not executed successfully'

    def test_checkSites(self, dirac_job):
        confirm = execute('checkSites()', return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'checkSites command not executed successfully'

    def test_bkMetaData(self, dirac_job):
        confirm = execute('bkMetaData("")', return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_getDataset(self, dirac_job):
        confirm = execute('getDataset("LHCb/Collision09/Beam450GeV-VeloOpen-MagDown/Real Data + RecoToDST-07/10/DST","","Path","","","")', return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

    def test_checkTier1s(self, dirac_job):
        confirm = execute('checkTier1s()', return_raw_dict=True)
        logger.info(confirm)
        assert confirm['OK'], 'Command not executed successfully'

# Problematic tests

    def test_getInputDataCatalog(self, dirac_job):
        confirm = execute('getInputDataCatalog("%s","","")' % dirac_job.get_file_lfn, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['Message'] == 'Failed to access all of requested input data' or confirm['Message'] == 'Could not access any requested input data', 'Command not executed successfully'

    def test_getLHCbInputDataCatalog(self, dirac_job):
        confirm = execute('getLHCbInputDataCatalog("%s",0,"","")' % dirac_job.get_file_lfn, return_raw_dict=True)
        logger.info(confirm)
        assert confirm['Message'] == 'Failed to access all of requested input data' or confirm['Message'] == 'Could not access any requested input data', 'Command not executed successfully'

