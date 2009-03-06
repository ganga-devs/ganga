from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
from GangaLHCb.Lib.Gaudi.GaudiLSFRunTimeHandler import GaudiLSFRunTimeHandler

class TestGaudiLSFRunTimeHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=DaVinci())
        j.inputsandbox = [File(name='dummy.in')]
        self.app = j.application._impl
        self.extra = GaudiExtras()
        self.extra.dataopts = '###DATAOPTS###'
        self.extra.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.extra._userdlls = ['dummy.dll']
        self.extra._merged_pys = ['dummy.merged.py']
        self.extra._subdir_pys = {} # I don't know the proper format for these
        self.extra.opts_pkl_str = 'DUMMYPKLSTR'
        self.rth = GaudiLSFRunTimeHandler()

    def test_GaudiLSFRunTimeHandler_prepare(self):
        stdjobconfig = self.rth.prepare(self.app,self.extra,None,None)
        assert len(stdjobconfig.getSandboxFiles()) == 2, 'inputsandbox error'
        l = len(stdjobconfig.getOutputSandboxFiles())
        assert  l == 3, 'outputsandbox error'

    def test_GaudiLSFRunTimeHandler_master_prepare(self):
        stdjobconfig = self.rth.master_prepare(self.app,self.extra)
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

    def test_GaudiLSFRunTimeHandler_create_runscript(self):
        self.app.user_release_area = 'TestURA'
        self.app.package = 'TestPackage'
        extra = GaudiExtras()
        extra.outputdata = ['test1.out','test2.out']
        script = self.rth.create_runscript(self.app,extra)
        strs = ['TestURA','TestPackage', str(extra.outputdata)]
        for s in strs:
            assert script.rfind(s) >= 0, 'script should contain %s' % s

