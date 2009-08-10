import os
import shutil
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.Gaudi import GaudiExtras
from GangaLHCb.Lib.Gaudi.Gaudi import Gaudi
from GangaLHCb.Lib.Gaudi.GaudiUtils import available_apps
from GangaTest.Framework.utils import read_file,failureException
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

class TestGaudi(GangaGPITestCase):

    def setUp(self):
        pass

    def test_Gaudi__auto__init__(self):
        dv = DaVinci()
        assert dv._impl.version, 'version not set automatically'
        assert dv._impl.platform, 'platform not set automatically'
        assert dv._impl.package, 'package not set automatically'
        assert dv._impl.user_release_area, 'ura not set automatically'

    def test_Gaudi_master_configure(self):
        job = Job(application=Gauss(optsfile='./Gauss.opts'))
        gauss = job.application
        job.inputdata = ['dummy1.in','dummy2.in']
        job.outputdata = ['Gauss.sim']
        job.outputsandbox = ['GaussHistos.root','GaussMonitor.root']
        inputs,extra = gauss._impl.master_configure()
        # provide basic test of where output goes - a more complete test is
        # run on the PythonOptionsParser methods.
        ok = extra.outputsandbox.count('GaussHistos.root') > 0 and \
             extra.outputsandbox.count('GaussMonitor.root') > 0
        assert ok, 'outputsandbox error'
        assert extra.outputdata.count('Gauss.sim') > 0, 'outputdata error'
        assert extra.master_input_buffers['options.pkl'] is not None

    # this method currently does nothing
    #def test_Gaudi_configure(self):

    # not much to check here...as this method simply runs checks itself
    #def test_Gaudi__check_inputs(self):

    # Andrew's new method...hopefully he can provide a unit test for it
    #def test_Gaudi_readInputData(self):

