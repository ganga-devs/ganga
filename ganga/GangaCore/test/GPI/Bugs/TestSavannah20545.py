from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah10016(GangaUnitTest):

    def test_a_TestFilePaths(self):
        from GangaCore.GPI import File

        # no absolute path prepended
        # 1. absolute unix paths
        path = '/home/ddd/dsgg'
        f = File(path)
        assert f.name == path

        # 2. URL:/path
        path = 'LFN:/lhcb/'
        f = File(path)
        assert f.name == path

        # 3. [PROTOCOL:][SETYPE:]/path/to/file
        path='PFN:castor:/castor/cern.ch/grid/lhcb/production/DC06/v1-lumi2/00001394/DST'
        f = File(path)
        assert f.name == path

        # 4. http://www.cern.ch/index.html
        path = 'LFN:/lhcb/'
        f = File(path)
        assert f.name == path

        # NOTE: Remove the tests because File now doesn't expand if the file isn't present
        ## The following should be expanded
        #path = 'file.txt'
        #f = File(path)
        #assert not f.name == path

        #path = 'dir/file.txt'
        #f = File(path)
        #assert not f.name == path

        #path = 'L/F:foobar'
        #f = File(path)
        #assert not f.name == path

        #URL should start with either an underscore or character
        #path = '123:/foobar'
        #f = File(path)
        #assert not f.name == path
