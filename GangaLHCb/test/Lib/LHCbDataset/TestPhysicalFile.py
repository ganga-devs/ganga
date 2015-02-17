from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.Files.PhysicalFile import *

class TestPhysicalFile(GangaGPITestCase):

    def test_full_expand_filename(self):
        import os
        pwd = os.path.abspath('.')
        name = 'test.txt'
        full_name = pwd + '/' + name
        assert full_expand_filename(name) == full_name, 'name not expanded'
        try:            
            full_expand_filename('lfn:'+name)
            raise 'using lfn should have raised exception'
        except:
            pass

    #test the rest in GPI tests
