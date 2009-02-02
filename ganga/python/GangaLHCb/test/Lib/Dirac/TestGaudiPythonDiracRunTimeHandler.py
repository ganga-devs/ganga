from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Dirac.GaudiPythonDiracRunTimeHandler import \
     GaudiPythonDiracRunTimeHandler
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras

class TestGaudiPythonDiracRunTimeHandler(GangaGPITestCase):
    
    def setUp(self):
        j = Job(application=GaudiPython(),backend=Dirac())
        j.inputsandbox = ['dummy.in']
        j.application.script = ['dummy.py']
        self.app = j.application._impl
        self.app.platform = config['DIRAC']['AllowedPlatforms'][0]
        self.extra = GaudiExtras()
        self.app.dataopts = '###DATAOPTS###'
        j.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.rth = GaudiPythonDiracRunTimeHandler()

    def test_GaudiPythonDiracRunTimeHandler_master_prepare(self):
        stdjobconfig = self.rth.master_prepare(self.app,self.extra)
        isbox = stdjobconfig.getSandboxFiles()
        found_in = False
        for f in isbox:
            print 'f = ', f
            if f.name.rfind('dummy.in') >= 0: found_in = True

    def test_GaudiPythonDiracRunTimeHandler_prepare(self):
        stdjobconfig = self.rth.prepare(self.app,self.extra,None,None)
        isbox = stdjobconfig.getSandboxFiles()
        found_gpwrap = False
        found_dtopts = False
        found_gwrapp = False
        for f in isbox:
            if f.name == 'gaudiPythonwrapper.py': found_gpwrap = True
            if f.name == 'data.opts': found_dtopts = True
            if f.name == 'GaudiWrapper.py': found_gwrapp = True

        assert found_gpwrap, 'gaudiPythonwrapper.py not in sandbox'
        assert found_dtopts, 'data.opts not in sandbox'
        assert found_gwrapp, 'GaudiWrapper.py not in sandbox'

        l = len(stdjobconfig.getOutputSandboxFiles())
        assert  l == 3, 'outputsandbox error'

    # not sure how to test the next 2 yet
    #def test_GaudiPythonDiracRunTimeHandler__DiracWrapper(self):
    #def test_GaudiPythonDiracRunTimeHandler_create_wrapperscript(self):
