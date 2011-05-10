import os
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
from GangaLHCb.Lib.Gaudi.GaudiPython import GaudiPython

class TestGaudiPython(GangaGPITestCase):

    def setUp(self):
        job = Job(application=GaudiPython())
        gp = job.application
        gp._impl._auto__init__()
        gp.script = [File('dummy.script')]
        job.inputdata = ['pfn:dummy1.in','pfn:dummy2.in']               
        self.gp = gp._impl
        self.gp.master_configure()
        self.job = job

    def test_GaudiPython__auto__init__(self):
        assert self.gp.project, 'project not set automatically'
        assert self.gp.version, 'version not set automatically'
        assert self.gp.platform, 'platform not set automatically'
        assert not self.gp.user_release_area

    def test_GaudiPython_master_configure(self):
        gp = self.gp
        #gp.master_configure() # must call this in set up for configure to work
        assert gp.extra.inputdata == self.job.inputdata._impl, 'inputdata err'
        found_script = False
        for f in gp.extra.master_input_files:
            print 'f.name =', f.name
            if f.name.rfind('dummy.script') >= 0:
                found_script = True
                break
        assert found_script, 'script not in sandbox'
        

    def test_GaudiPtython_configure(self):
        gp = self.gp
        gp.configure(None)
        assert gp.extra.input_buffers['gaudipython-wrapper.py'] is not None

    # not much to check here...as this method simply runs checks itself
    #def test_GaudiPython__check_inputs(self):

