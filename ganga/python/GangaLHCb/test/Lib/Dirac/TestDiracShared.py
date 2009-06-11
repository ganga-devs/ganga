import os
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Dirac.DiracShared import *

class TestDiracShared(GangaGPITestCase):

    # hard to unit test anything else in this file

    def test_getGenericRunScript(self):
        j = Job(backend=Dirac())
        f = getGenericRunScript(j._impl)
        should_be = os.path.join(j._impl.getInputWorkspace().getPath(),
                                 'diracJobMain.py')
        assert f == should_be, 'file name not correct'
        assert os.path.exists(should_be), 'file not copied properly'
       
