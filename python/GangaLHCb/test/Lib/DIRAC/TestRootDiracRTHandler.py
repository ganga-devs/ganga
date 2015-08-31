from GangaTest.Framework.tests import GangaGPITestCase

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.RTHandlers.LHCbRootDiracRunTimeHandler import LHCbRootDiracRunTimeHandler


class TestRootDiracRTHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=Root(), backend=Dirac())
        import Ganga.Utility.Config
        if not getConfig('Output')['ForbidLegacyInput']:
            j.inputsandbox = [File(name='dummy.in')]
        else:
            j.inputfiles = [LocalFile('dummy.in')]
        #j.inputsandbox = [File(name='dummy.in')]
        j.outputfiles = ['dummy1.out', 'dummy2.out', 'dummy3.out']
        self.j = j
        self.app = j.application._impl
        from GangaLHCb.Lib.RTHandlers.LHCbRootDiracRunTimeHandler import LHCbRootDiracRunTimeHandler
        self.rth = LHCbRootDiracRunTimeHandler()

    def test_RootDiracRTHandler_master_prepare(self):
        os.system('touch /tmp/testrdrth_mp.C')
        self.j.application.script = '/tmp/testrdrth_mp.C'
        self.app.prepare()
        stdjobconfig = self.rth.master_prepare(self.app, None)
        os.system('rm -f /tmp/testrdrth_mp.C')
        isbox = stdjobconfig.getSandboxFiles()
        print("inputsandbox = ", isbox)
        assert len(isbox) == 2, 'incorrect number of files in sandbox'
        assert (isbox[0].name == 'dummy.in') or (
            isbox[1].name == 'dummy.in'), 'incorrect file name'

    def test_RootDiracRTHandler_prepare(self):
        os.system('touch /tmp/testrdrth_mp.C')
        self.j.application.script = '/tmp/testrdrth_mp.C'
        self.app.prepare()
        stdjobconfig = self.rth.prepare(
            self.app, None, None, self.rth.master_prepare(self.app, None))
        os.system('rm -f /tmp/testrdrth_mp.C')
        l = len(stdjobconfig.getOutputSandboxFiles())
        print("outputsandbox = ", stdjobconfig.getOutputSandboxFiles())
        assert l == 4, 'outputsandbox error'
