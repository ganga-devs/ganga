from GangaTest.Framework.tests import GangaGPITestCase
#from GangaLHCb.Lib.Gaudi.Francesc import GaudiExtras
from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
#from GangaLHCb.Lib.Gaudi.GaudiJobConfig import GaudiJobConfig

import Ganga.Utility.Config.Config
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler


class TestGaudiRunTimeHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=DaVinci())
        j.prepare()
        from Ganga.Utility.Config import getConfig
        if getConfig('Output')['ForbidLegacyInput']:
            j.inputfiles = [LocalFile(name='dummy.in')]
        else:
            j.inputsandbox = [File(name='dummy.in')]
        self.app = j.application._impl
        #self.extra = GaudiExtras()
        # self.extra.master_input_buffers['master.buffer'] = '###MASTERBUFFER###'
        #self.extra.master_input_files = [File(name='master.in')]
        # self.extra.input_buffers['subjob.buffer'] = '###SUBJOBBUFFER###'
        self.input_files = [File(name='subjob.in'), File(
            FileBuffer('subjob.buffer', '###SUBJOBBUFFER###').create().name)]
        self.appmasterconfig = StandardJobConfig(inputbox=[File(name='master.in'), File(
            FileBuffer('master.buffer', '###MASTERBUFFER###').create().name)])
        j.outputfiles = ['dummy1.out', 'dummy2.out', 'dummy3.out']
        self.rth = LHCbGaudiRunTimeHandler()

    def test_GaudiRunTimeHandler_master_prepare(self):
        stdjobconfig = self.rth.master_prepare(self.app, self.appmasterconfig)
        # should have master.buffer, master.in and options.pkl and dummy.in
        print("sandbox =", stdjobconfig.getSandboxFiles())
        print(
            "sandbox =", [file.name for file in stdjobconfig.getSandboxFiles()])
        assert len(stdjobconfig.getSandboxFiles()) == 3

    def test_GaudiRunTimeHandler_prepare(self):
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
        assert len(stdjobconfig.getSandboxFiles()) == 4, 'inputsandbox error'
        l = len(stdjobconfig.getOutputSandboxFiles())
        print("outputsandbox =", stdjobconfig.getOutputSandboxFiles())
        assert l == 4, 'outputsandbox error'
