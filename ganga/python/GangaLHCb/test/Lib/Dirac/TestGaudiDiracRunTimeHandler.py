from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Dirac.GaudiDiracRunTimeHandler import \
     GaudiDiracRunTimeHandler
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras

class TestGaudiDiracRunTimeHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=DaVinci(),backend=Dirac())
        j.inputsandbox = [File(name='dummy.in')]
        j.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.j = j
        self.app = j.application._impl
        self.app.platform = config['DIRAC']['AllowedPlatforms'][0]
        self.app.extra = GaudiExtras()
        self.app.extra.dataopts = '###DATAOPTS###'
        self.app.extra.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.app.extra._userdlls = ['dummy.dll']
        self.app.extra._merged_pys = ['dummy.merged.py']
        self.app.extra._subdir_pys = {} 
        self.app.extra.opts_pkl_str = 'DUMMYPKLSTR'
        self.rth = GaudiDiracRunTimeHandler()

    
    def test_GaudiDiracRunTimeHandler_master_prepare(self):
        stdjobconfig = self.rth.master_prepare(self.app,None)
        isbox = stdjobconfig.getSandboxFiles()
        found_dll = False
        found_mpy = False
        found_in = False
        for f in isbox:
            if f.name == 'dummy.in': found_in = True
            elif f.subdir == 'lib' and f.name == 'dummy.dll': found_dll = True
            elif f.subdir == 'python' and f.name == 'dummy.merged.py':
                found_mpy = True

        assert found_in, 'job.inputsandbox no properly added to input sandbox'
        assert found_dll, 'user_dlls not properly added to input sandbox'
        assert found_mpy, 'user_merged_pys not properly added to input sandbox'

    def test_GaudiDiracRunTimeHandler_prepare(self):
        stdjobconfig = self.rth.prepare(self.app,None,None,None)
        isbox = stdjobconfig.getSandboxFiles()
        found_opts = False
        found_gwrp = False
        for f in isbox:
            if f.name == 'dataopts.opts': found_opts = True
            if f.name == 'GaudiWrapper.py': found_gwrp = True
        assert found_opts, 'dataopts.opts not added to sandbox'
        assert found_gwrp, 'GaudiWrapper.py not added to sandbox'
        l = len(stdjobconfig.getOutputSandboxFiles())
        assert  l == 3, 'outputsandbox error'

    # not sure what's testable here
    #def test_GaudiDiracRunTimeHandler__DiracWrapper(self):
