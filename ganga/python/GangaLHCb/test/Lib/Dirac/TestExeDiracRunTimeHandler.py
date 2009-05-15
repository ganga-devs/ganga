from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Dirac.ExeDiracRunTimeHandler import ExeDiracRunTimeHandler

class TestExeDiracRunTimeHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=Executable(),backend=Dirac())
        j.inputsandbox = [File(name='dummy.in')]
        j.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.j = j
        self.app = j.application._impl
        self.rth = ExeDiracRunTimeHandler()

    def test_ExeDiracRunTimeHandler_master_prepare(self):
        stdjobconfig = self.rth.master_prepare(self.app,None)
        isbox = stdjobconfig.getSandboxFiles()
        assert len(isbox) == 1, 'incorrect number of files in sandbox'
        assert isbox[0].name == 'dummy.in', 'incorrect file name'
        
    def test_ExeDiracRunTimeHandler_prepare(self):
        stdjobconfig = self.rth.prepare(self.app,None,None,None)
        l = len(stdjobconfig.getOutputSandboxFiles())
        assert  l == 3, 'outputsandbox error'
        #ds = stdjobconfig.script # add tests on this later
        
