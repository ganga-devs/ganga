from GangaTest.Framework.tests import GangaGPITestCase
import Ganga.Utility.Config
from Ganga.Core import BackendError
from GangaLHCb.Lib.Dirac.RootVersions import *

config = Ganga.Utility.Config.getConfig('DIRAC')

class TestRootVersions(GangaGPITestCase):

    def setUp(self):
        # make sure config is OK, otherwise no point running the tests
        assert len(config['RootVersions'])

    def test_getDaVinciVersion(self):
        v = listRootVersions()[-1]
        assert getDaVinciVersion(v), 'valid version should be ok'
        assert getDaVinciVersion(), 'default shoould be ok'
        assert getDaVinciVersion(None), 'None should still be ok'

    def test_listRootVersions(self):
        list = listRootVersions()
        assert len(list) == len(config['RootVersions']), 'incorrect length'
