from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler
#from GangaLHCb.Lib.Gaudi.Francesc import GaudiExtras
from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
from GangaLHCb.test import *
from Ganga.GPIDev.Lib.File.File import File
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
#from GangaLHCb.Lib.Gaudi.GaudiJobConfig import GaudiJobConfig

class TestGaudiDiracRTHandler(GangaGPITestCase):

    def setUp(self):
        j = Job(application=DaVinci(),backend=Dirac())
        j.prepare()
        j.inputsandbox = [File(name='dummy.in')]
        j.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.j = j
        self.app = j.application._impl
        self.app.platform = getDiracAppPlatform()
        #self.extra = GaudiExtras()
        #self.extra.master_input_buffers['master.buffer'] = '###MASTERBUFFER###'
        #self.extra.master_input_files = [File(name='master.in')._impl]
        #self.extra.input_buffers['subjob.buffer'] = '###SUBJOBBUFFER###'
        self.input_files = [File(name='subjob.in'),File(FileBuffer('subjob.buffer','###SUBJOBBUFFER###').create().name)]
        self.appmasterconfig = StandardJobConfig(inputbox=[File(name='master.in'),File(FileBuffer('master.buffer','###MASTERBUFFER###').create().name)])
        #self.extra.outputsandbox = ['dummy1.out','dummy2.out','dummy3.out']
        self.rth = GaudiDiracRTHandler()
    
    def test_GaudiDiracRTHandler_master_prepare(self):
        #app = self.app
        #app.extra = self.extra
        stdjobconfig = self.rth.master_prepare(self.app,self.appmasterconfig)
        # should have master.buffer, master.in and options.pkl
        print 'sandbox =', stdjobconfig.getSandboxFiles()
        assert len(stdjobconfig.getSandboxFiles()) == 3

    def test_GaudiDiracRTHandler_prepare(self):
        #app = self.app
        #app.extra = self.extra
        sjc = StandardJobConfig(inputbox=self.input_files)
        stdjobconfig = self.rth.prepare(self.app,sjc,self.appmasterconfig,StandardJobConfig())
        # should have subjob.in(buffer), data.opts and gaudiscript.py
        print "sandbox =",stdjobconfig.getSandboxFiles()
        assert len(stdjobconfig.getSandboxFiles()) == 4, 'inputsandbox error'
        l = len(stdjobconfig.getOutputSandboxFiles())
        assert  l == 3, 'outputsandbox error'

    # not sure what's testable here
    #def test_GaudiDiracRTHandler__create_dirac_script(self):
