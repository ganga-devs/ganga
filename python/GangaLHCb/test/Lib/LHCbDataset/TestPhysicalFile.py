from GangaTest.Framework.tests import GangaGPITestCase

try:
    import Ganga.Utility.Config.Config
    doConfig = not Ganga.Utility.Config.Config._after_bootstrap
except x:
    print(x)
    doConfig = True

if doConfig:
    from GangaLHCb.Lib.Files.PhysicalFile import *


class TestPhysicalFile(GangaGPITestCase):

    def test_full_expand_filename(self):
        from GangaLHCb.Lib.Files.PhysicalFile import full_expand_filename
        import os.path
        pwd = os.path.abspath('.')
        name = 'test.txt'
        full_name = pwd + '/' + name
        assert full_expand_filename(name) == full_name, 'name not expanded'
        try:
            full_expand_filename('lfn:' + name)
            raise RuntimeError('using lfn should have raised exception')
        except:
            pass

    # test the rest in GPI tests
