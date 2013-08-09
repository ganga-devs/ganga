from GangaTest.Framework.tests import GangaGPITestCase

from GangaTest.Framework.utils import sleep_until_completed,file_contains,write_file,sleep_until_state
from Ganga.Utility.Config      import setConfigOption
from GangaLHCb.test import *
addDiracTestSubmitter()

ganga_path = os.path.abspath(os.path.dirname(__file__))
script_file = ganga_path + '/../python/GangaLHCb/test/GPI/Dirac/test.C'

class TestRootDirac(GangaGPITestCase):


    def testAllowedArchitecture(self):
        """Test the submission of root jobs on dirac"""

        setConfigOption('LHCb','ignore_version_check',False)
        config.ROOT.arch = 'x86_64-slc5-gcc43-opt'
        
        r = Root(script=script_file)

        j = Job(application=r, backend=DiracTestSubmitter())
        j.submit()
        sleep_until_completed(j)
        assert j.status == 'completed', 'Job should complete'
        
    def testNotAllowedArchitecture(self):
        """Tests the architectures not allowed by dirac"""
        
        setConfigOption('LHCb','ignore_version_check',False)
        config.ROOT.arch = 'x86_64-slc5-gcc43-opt-not-a-valid-version'
        
        r = Root(script=script_file)
        j = Job(application=r, backend=Dirac())
        
        try:
            j.submit()
            assert False, 'Exception must be thrown'
        except JobError, e:
            pass
        
        assert j.status == 'new', 'Job must be rolled back to the new state'
        
    def testProperSubmit(self):
        
        config.ROOT.arch = 'x86_64-slc5-gcc43-opt'
        
        j = Job(application=Root(script=script_file), backend=Dirac())
        j.submit()
        
        sleep_until_state(j, state = 'submitted')
        assert j.status == 'submitted', 'Job should submit'
        j.kill()
        
