from __future__ import print_function
import os
from GangaTest.Framework.tests import GangaGPITestCase
#from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.Applications.GaudiPython import GaudiPython


class TestGaudiPython(GangaGPITestCase):

    def setUp(self):
        self.job = Job(application=GaudiPython())
        gp = self.job.application
        gp._impl._auto__init__()
        f = open('dummy.script', 'w')
        f.write('')
        f.close()
        gp.script = [File('dummy.script')]
        self.job.inputdata = ['pfn:dummy1.in', 'pfn:dummy2.in']
        self.gp = gp._impl
        self.master_config = self.gp.master_configure()[1]
        #self.job = job

    def test_GaudiPython__auto__init__(self):
        assert self.gp.project, 'project not set automatically'
        assert self.gp.version, 'version not set automatically'
        assert self.gp.platform, 'platform not set automatically'
        #print("%s" % str(self.gp.user_release_area))
        #assert not self.gp.user_release_area
        assert os.path.basename(self.gp.user_release_area) == 'cmtuser'

    def test_GaudiPython_prepare(self):
        g = self.job.application
        g.prepare()
        assert g.is_prepared is not None, 'is_prepared not correctly set'
        g.is_prepared.ls()
        share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                  'shared',
                                  getConfig('Configuration')['user'],
                                  g.is_prepared.name,
                                  'inputsandbox')
        os.system(
            'cd ' + share_path + ';tar -xzvf _input_sandbox_' + g.is_prepared.name + '.tgz')
        assert os.path.exists(os.path.join(share_path, 'dummy.script'))

    def test_GaudiPython_unprepare(self):
        g = self.job.application
        g.prepare()
        assert g.is_prepared is not None, 'is_prepared not correctly set'
        g.unprepare()
        assert g.is_prepared is None, 'is_prepared not correctly unset'

# def test_GaudiPython_master_configure(self):
##         gp = self.gp
# gp.master_configure() # must call this in set up for configure to work
# assert gp.extra.inputdata == self.job.inputdata._impl, 'inputdata err'
##         found_script = False
# for f in self.master_config.inputbox:
##             print('f.name =', f.name)
# if f.name.rfind('dummy.script') >= 0:
##                 found_script = True
# break
##         assert found_script, 'script not in sandbox'

    def test_GaudiPtython_configure(self):
        gp = self.gp
        subconfig = gp.configure(None)[1]
        input_files = [f.name for f in subconfig.inputbox]
        print('input_files =', input_files)
        assert 'gaudipython-wrapper.py' in input_files, 'didnt find gaudipython wrapper'
        f = subconfig.inputbox[input_files.index('gaudipython-wrapper.py')]
        # f=file(path,'r')
        buffer = f.getContents()
        # f.close()
        assert buffer is not ''

    # not much to check here...as this method simply runs checks itself
    # def test_GaudiPython__check_inputs(self):
