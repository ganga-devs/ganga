from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset.LogicalFile import *

class TestLogicalFile(GangaGPITestCase):

    #def test_get_dirac_space_tokens(self):

    def test_get_result(self):
        get_result('result = {"OK": True}','','')
        try:
            get_result('result = {"OK": False}','','')
            raise 'should have thrown exception'
        except:
            pass
    
    def test_strip_filename(self):
        name = 'test.txt'
        assert strip_filename(name) == name, 'name altered'
        assert strip_filename('lfn:'+name) == name, 'lfn not stripped'
        try:            
            strip_filename('pfn:'+name)
            raise 'using pfn should have raised exception'
        except:
            pass

    # test the rest in the GPI

        
