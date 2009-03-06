import inspect
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
import Ganga.Utility.Config 
from GangaLHCb.Lib.Gaudi.GaudiPythonLSFRunTimeHandler \
     import GaudiPythonLSFRunTimeHandler

class TestGaudiPythonLSFRunTimeHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=GaudiPython())
        j.inputsandbox = ['dummy.in']
        j.application.script = ['dummy.py']
        self.app = j.application._impl
        self.extra = GaudiExtras()
        self.app.dataopts = '###DATAOPTS###'
        j.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.rth = GaudiPythonLSFRunTimeHandler()

    def test_GaudiPythonLSFRunTimeHandler_master_prepare(self):
        stdjobconfig = self.rth.master_prepare(self.app,self.extra)
        isbox = stdjobconfig.getSandboxFiles()
        found_in = False
        for f in isbox:
            print 'f = ', f
            if f.name.rfind('dummy.in') >= 0: found_in = True

        assert found_in, 'job.inputsandbox no properly added to input sandbox'

    def test_GaudiPythonLSFRunTimeHandler_prepare(self):
        stdjobconfig = self.rth.prepare(self.app,self.extra,None,None)
        isbox = stdjobconfig.getSandboxFiles()
        assert len(stdjobconfig.getSandboxFiles()) == 3, 'inputsandbox error'
        l = len(stdjobconfig.getOutputSandboxFiles())
        assert  l == 3, 'outputsandbox error'

    def test_GaudiLSFRunTimeHandler_create_runscript(self):
        script = self.rth.create_runscript(self.app,None)
        config = Ganga.Utility.Config.getConfig('LHCb')
        strs = ['gaudiPythonwrapper.py']
        for s in strs:
            assert script.rfind(s) >= 0, 'script should contain %s' % s
