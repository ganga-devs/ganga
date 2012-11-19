from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.DIRAC.RootDiracRTHandler import RootDiracRTHandler

class TestRootDiracRTHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=Root(),backend=Dirac())
        j.inputsandbox = [File(name='dummy.in')]
        j.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.j = j
        self.app = j.application._impl
        self.rth = RootDiracRTHandler()

    def test_RootDiracRTHandler_master_prepare(self):
        os.system('touch /tmp/testrdrth_mp.C')
        self.j.application.script = '/tmp/testrdrth_mp.C'
        stdjobconfig = self.rth.master_prepare(self.app,None)
        os.system('rm -f /tmp/testrdrth_mp.C')
        isbox = stdjobconfig.getSandboxFiles()
        assert len(isbox) == 1, 'incorrect number of files in sandbox'
        assert isbox[0].name == 'dummy.in', 'incorrect file name'
        
    def test_RootDiracRTHandler_prepare(self):
        stdjobconfig = self.rth.prepare(self.app,None,None,None)
        l = len(stdjobconfig.getOutputSandboxFiles())
        assert  l == 3, 'outputsandbox error'
