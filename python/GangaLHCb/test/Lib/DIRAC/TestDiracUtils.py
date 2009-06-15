import os
import sys
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
import Ganga.Utility.Config
from GangaLHCb.Lib.DIRAC.DiracUtils import *

#config = Ganga.Utility.Config.getConfig('DIRAC')

class TestDiracUtils(GangaGPITestCase):

    def test_result_ok(self):
        assert not result_ok(None), 'None should return False'
        assert not result_ok(66), 'Non-{} should return False'
        assert not result_ok({}), '{} w/ no OK should return False'
        assert not result_ok({'OK':False}), 'OK not handled properly'
        assert result_ok({'OK':True}), 'OK not handled properly'

    def test_grid_proxy_ok(self):
        config.DIRAC.extraProxytime = '0'
        assert grid_proxy_ok() is None, 'proxy should be valid'
        config.DIRAC.extraProxytime = '1e10'
        assert grid_proxy_ok() is not None, 'proxy should not be valid'

    #def test_mangle_job_name(self):
