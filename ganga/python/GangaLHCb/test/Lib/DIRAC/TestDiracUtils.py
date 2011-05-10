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

    #def test_mangle_job_name(self):
