from GangaTest.Framework.tests import GangaGPITestCase

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.LHCbDataset.OutputData import *


class TestOutputData(GangaGPITestCase):

    def test___len__(self):
        d = OutputData(['a', 'b'])
        assert len(d) == 2

    # test the rest in the GPI
