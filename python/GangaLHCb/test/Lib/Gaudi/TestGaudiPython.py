import os
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
from GangaLHCb.Lib.Gaudi.GaudiPython import GaudiPython

class TestGaudiPython(GangaGPITestCase):

    def setUp(self):
        job = Job(application=GaudiPython())
        gp = job.application
        gp._impl._auto__init__() # for some reason, this must be called in this
                                 # test...but not in ganga. FIXME?
        job.inputdata = ['dummy1.in','dummy2.in']               
        self.gp = gp._impl
        self.job = job
        self.gp.master_configure()
        self.gp.configure(None)

    def test_GaudiPython__auto__init__(self):
        assert self.gp.project, 'project not set automatically'
        assert self.gp.version, 'version not set automatically'
        assert self.gp.platform, 'platform not set automatically'

    def test_GaudiPython_master_configure(self):
        gp = self.gp
        assert gp.extra.inputdata == self.job.inputdata._impl, 'inputdata err'
        assert gp.package, 'no package found for application'

    def test_GaudiPtython_configure(self):
        gp = self.gp
        assert gp.dataopts.rfind('dummy1.in') >= 0 and \
               gp.dataopts.rfind('dummy2.in') >= 0, 'dataopts string error'

    # not much to check here...as this method simply runs checks itself
    #def test_GaudiPython__check_inputs(self):

