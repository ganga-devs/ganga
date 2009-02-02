from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Gaudi.CMTscript import *

class TestCMTscript(GangaGPITestCase):
    """Tests methods defined in GangaLHCb/Lib/Gaudi/CMTscript.py"""

    def test_parse_master_package(self):
        # this test will pass for current (6/11/2008) version of this method.
        # this method is currently not used and the behavior is a bit odd
        # (eg. it returns different types for / vs ' ' sepearated lists).
        # if this method is ever brought back into usage, this test may need
        # to be modified.
        val = parse_master_package('a/b/c')
        assert val == ['a','b','c'], 'splitting dir names (3) is broken'
        val = parse_master_package('a/b')
        assert val == ['','a','b'], 'splitting dir names (2) is broken'
        val = parse_master_package('a b c')
        assert val == ('c','a','b'), 'splitting \' \' strings (3) is broken'
        val = parse_master_package('a b')
        assert val == ('','a','b'), 'splitting \' \' strings (2) is broken'

    def test_CMTscript(self):
        cmd = "###CMT### -h"
        dv = DaVinci()
        assert CMTscript(dv._impl,cmd), 'write permisions error'

