from GangaTest.Framework.tests import GangaGPITestCase
from GangaDirac.Lib.RTHandlers.ExeDiracRTHandler import ExeDiracRTHandler

class TestExeDiracRTHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=Executable(),backend=Dirac())
        j.inputsandbox = [File(name='dummy.in')]
        j.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.j = j
        self.app = j.application._impl
        self.rth = ExeDiracRTHandler()

    def test_ExeDiracRTHandler_master_prepare(self):
        stdjobconfig = self.rth.master_prepare(self.app,None)
        isbox = stdjobconfig.getSandboxFiles()
        assert len(isbox) == 1, 'incorrect number of files in sandbox'
        assert isbox[0].name == 'dummy.in', 'incorrect file name'
        
    def test_ExeDiracRTHandler_prepare(self):
        stdjobconfig = self.rth.prepare(self.app,None,None,None)
        l = len(stdjobconfig.getOutputSandboxFiles())
        assert  l == 3, 'outputsandbox error'
        # writing of the script is testing in the DiracScript.py tests
        
