from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset.OutputData import *

class TestOutputData(GangaGPITestCase):

    def test___len__(self):
        d = OutputData(['a','b'])
        assert len(d) == 2

    # test the rest in the GPI

        
