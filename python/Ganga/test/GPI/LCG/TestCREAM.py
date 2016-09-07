import re
import subprocess
import pytest

from Ganga.Utility.Config import getConfig

from Ganga.testlib.mark import external

from Ganga.Utility.Config.Config import getConfig
from Ganga.testlib.GangaUnitTest import load_config_files, clear_config
load_config_files()
glite_status = getConfig('LCG')['GLITE_ENABLE']
clear_config()

@external
@pytest.mark.skipif(glite_status == False, reason="Cannot run test GLITE not enabled")
def test_job_kill(gpi):
    from Ganga.GPI import Job, CREAM

    vo = getConfig('LCG')['VirtualOrganisation']
    call = subprocess.Popen(['lcg-infosites', 'ce', 'cream', '--vo', vo], stdout=subprocess.PIPE)
    stdout, stderr = call.communicate()

    # Based on output of:
    #
    # #   CPU    Free Total Jobs      Running Waiting ComputingElement
    # ----------------------------------------------------------------
    #   19440    2089      17760        17351     409 arc-ce01.gridpp.rl.ac.uk:2811/nordugrid-Condor-grid3000M
    #    3240       0       1594         1250     344 carceri.hec.lancs.ac.uk:8443/cream-sge-grid
    #    1176      30       1007          587     420 ce01.tier2.hep.manchester.ac.uk:8443/cream-pbs-long
    #
    # Select the CREAM CEs (URL path starts with '/cream') and how many free slots they have
    ces = re.findall(r'^\s*\d+\s*(?P<free>\d+)\s*\d+\s*\d+\s*\d+\s*(?P<ce>[^:/\s]+:\d+/cream.*)$', stdout, re.MULTILINE)
    # Grab the one with the most empty slots
    ce = sorted(ces)[-1][1]

    j = Job()
    j.backend = CREAM(CE=ce)
    j.submit()
    j.kill()
