import os
import sys
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.GPIDev.Credentials import GridProxy
from Ganga.Core import BackendError
import Ganga.Utility.Config 
config = Ganga.Utility.Config.getConfig('DIRAC')

class TestDirac(GangaGPITestCase):

    # nothing to test
    #def test_Dirac__getDiracScript(self):

    # tested by submit
    #def test_Dirac__submit(self):

    def test_Dirac_submit(self):
        j = Job(backend=Dirac())
        j.backend.diracOpts = '# 87 71 66 68'
        j.submit()
        # this was already called in setUp
        script = open(os.path.join(j._impl.getInputWorkspace().getPath(),'dirac-script.py')).read()
        assert script.find('# 87 71 66 68') >= 0, 'diracOpts not found'
        # TODO: expand this test to check more API commands in job def
        
    # this method is a wrapper for _submit
    #def test_Dirac_resubmit(self):

    # not much to test here
    #def test_Dirac_reset(self):

    def test_Dirac_kill(self):
        j = Job(backend=Dirac())
        j.submit()
        try:
            j.backend._impl.kill()
        except BackendError:
            assert False, 'BackendError should not have been thrown.'
        #j.kill()
        
    def test_Dirac_peek(self):
        j = Job(backend=Dirac())
        j.submit()
        stdout = sys.stdout
        tmpdir = tempfile.mktemp()
        f = tempfile.NamedTemporaryFile()
        sys.stdout = f
        j.backend._impl.peek()        
        sys.stdout = stdout
        j.kill()
        f.flush()
        fstdout = open(f.name,'r')
        s = fstdout.read()
        assert s, 'Something should have been written to std output'
        f.close()

    # these must be tested in the GPI tests (require a completed job)
    #def test_Dirac_getOutputSandbox(self):
    #def test_Dirac__getOutputSandbox(self):
    #def test_Dirac_getOutputData(self):
    #def test_Dirac_getOutputDataLFNs(self):    
    #def test_Dirac_debug(self):
        
    # test this in GPI
    #def test_Dirac_updateMonitoringInformation(self):

#    def test_Dirac_execAPI(self):
#        cmd = 'print 87'
#        from GangaLHCb.Lib.Backends.Dirac import Dirac
#        assert Dirac.execAPI(cmd) == 87, 'DIRAC API commands broken'
    
