import time
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *

class TestLHCbDataset(GangaGPITestCase):

    def setUp(self):
        self.file_name = '/lhcb/production/DC06/phys-v2-lumi2/00001889/DST/' \
                         '0000/00001889_00000003_5.dst'

    def test_LHCbDataset___len__(self):
        ds = LHCbDataset()
        assert len(ds) == 0
        ds = LHCbDataset([LHCbDataFile('a')])
        assert len(ds) == 1

    def test_LHCbDataset_cacheOutOfDate(self):
        ds = LHCbDataset()
        ds.cache_date = 'Tue Jun 6 06:30:00 1944' # D-Day H-Hour
        assert ds.cacheOutOfDate(), 'cache should be out of date'
        ds.cache_date = time.asctime() # now
        assert not ds.cacheOutOfDate(), 'cache should be up to date'

    def test_LHCbDataset_isEmpty(self):
        ds = LHCbDataset()
        assert ds.isEmpty(), 'dataset is empty'
        ds = LHCbDataset(files=[LHCbDataFile('LFN:' + self.file_name)])
        assert not ds.isEmpty(), 'dataset is not empty'

    def test_LHCbDataset_updateReplicaCache(self):
        ds = LHCbDataset(files=[LHCbDataFile('LFN:' + self.file_name)])
        ds.cache_date = 'Tue Jun 6 06:30:00 1944' # D-Day H-Hour
        ds.updateReplicaCache()
        assert ds.files[0].replicas
        assert not ds.cacheOutOfDate()
        
    def test_string_dataset_shortcut(self):
        files = ['LFN:' + self.file_name, 'LFN:' + self.file_name]
        ds = string_dataset_shortcut(files,None)
        assert ds, 'dataset should not be None'
        assert len(ds) == 2, 'length should be 2 not %d' % len(ds)
        
