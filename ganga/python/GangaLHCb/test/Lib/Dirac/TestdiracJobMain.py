import os
import tempfile
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Dirac.diracJobMain import listdirs

class TestdiracJobMain(GangaGPITestCase):

    def test_listdirs(self):
        dir = os.path.dirname(__file__) + '/../python/GangaLHCb/Lib/'
        list = listdirs(dir)
        assert list.count(dir + 'Gaudi/Gaudi.py') == 1, 'Gaudi.py not found'
        assert list.count(dir + 'Gaudi/options/DSTMerger.opts') == 1, \
               'Subdir file not found'
