from GangaTest.Framework.tests import GangaGPITestCase
#from GangaLHCb.Lib.Gaudi.Francesc import GaudiExtras
from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
#from GangaLHCb.Lib.Gaudi.GaudiJobConfig import GaudiJobConfig
import Ganga.Utility.Config.Config
from GangaLHCb.Lib.RTHandlers.LHCbGaudiDiracRunTimeHandler import LHCbGaudiDiracRunTimeHandler


class TestLHCbGaudiDiracRunTimeHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=DaVinci(), backend=Dirac())
        j.prepare()
        from Ganga.Utility.Config import getConfig
        if getConfig('Output')['ForbidLegacyInput']:
            j.inputfiles = [LocalFile(namePattern='dummy.in')]
        else:
            j.inputsandbox = [File(name='dummy.in')]
        j.outputfiles = ['dummy1.out', 'dummy2.out', 'dummy3.out']
        self.j = j
        self.app = j.application._impl
        from GangaLHCb.test import getDiracAppPlatform
        self.app.platform = getDiracAppPlatform()
        #self.extra = GaudiExtras()
        # self.extra.master_input_buffers['master.buffer'] = '###MASTERBUFFER###'
        #self.extra.master_input_files = [File(name='master.in')._impl]
        # self.extra.input_buffers['subjob.buffer'] = '###SUBJOBBUFFER###'
        self.input_files = [File(name='subjob.in'), File(
            FileBuffer('subjob.buffer', '###SUBJOBBUFFER###').create().name)]
        self.appmasterconfig = StandardJobConfig(inputbox=[File(name='master.in'), File(
            FileBuffer('master.buffer', '###MASTERBUFFER###').create().name)])
        #self.extra.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.rth = LHCbGaudiDiracRunTimeHandler()

    def test_LHCbGaudiDiracRunTimeHandler_master_prepare(self):
        #app = self.app
        #app.extra = self.extra
        stdjobconfig = self.rth.master_prepare(self.app, self.appmasterconfig)
        # should have master.buffer, master.in and options.pkl
        # shouldn't that now be master.buffer master.in and inputsandbox?
        # rcurrie
        logger = Ganga.Utility.logging.getLogger()
        logger.info('sandbox = %s' % str(stdjobconfig.getSandboxFiles()))
        assert len(stdjobconfig.getSandboxFiles()) == 3

    def test_LHCbGaudiDiracRunTimeHandler_prepare(self):
        #app = self.app
        #app.extra = self.extra
        sjc = StandardJobConfig(
            inputbox=self.input_files, outputbox=['dummy1.out', 'dummy2.out', 'dummy3.out'])
        jobmasterconfig = StandardJobConfig()
        jobmasterconfig.outputdata = []
        stdjobconfig = self.rth.prepare(
            self.app, sjc, self.appmasterconfig, jobmasterconfig)
        # should have subjob.in(buffer), data.opts and gaudiscript.py
        print("sandbox =", stdjobconfig.getSandboxFiles())
        print(
            "sandbox =", [file.name for file in stdjobconfig.getSandboxFiles()])
        assert len(stdjobconfig.getSandboxFiles()) == 3, 'inputsandbox error'
        l = len(stdjobconfig.getOutputSandboxFiles())
        print("outputsandbox =", stdjobconfig.getOutputSandboxFiles())
        assert l == 3, 'outputsandbox error'

    # not sure what's testable here
    # def test_GaudiDiracRTHandler__create_dirac_script(self):
