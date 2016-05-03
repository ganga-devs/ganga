from GangaTest.Framework.tests import GangaGPITestCase

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.Applications.CMTscript import *


class TestCMTscript(GangaGPITestCase):

    """Tests methods defined in GangaLHCb/Lib/Gaudi/CMTscript.py"""

    def test_parse_master_package(self):
        from GangaLHCb.Lib.Applications.CMTscript import parse_master_package
        val = parse_master_package('a/b/c')
        assert val == ['a', 'b', 'c'], 'splitting dir names (3) is broken'
        val = parse_master_package('a/b')
        assert val == ['', 'a', 'b'], 'splitting dir names (2) is broken'
        val = parse_master_package('a b c')
        assert val == ('c', 'a', 'b'), 'splitting \' \' strings (3) is broken'
        val = parse_master_package('a b')
        assert val == ('', 'a', 'b'), 'splitting \' \' strings (2) is broken'

    def test_CMTscript(self):
        from GangaLHCb.Lib.Applications.CMTscript import CMTscript
        cmd = "###CMT### -h"
        dv = DaVinci()
        assert CMTscript(dv._impl, cmd) == 0, 'write permisions error'
