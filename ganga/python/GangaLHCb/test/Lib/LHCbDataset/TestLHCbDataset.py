import time
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset import string_dataset_shortcut, \
     string_datafile_shortcut

class TestLHCbDataset(GangaGPITestCase):

    def setUp(self):
        self.file_name = '/lhcb/production/DC06/phys-v2-lumi2/00001889/DST/' \
                         '0000/00001889_00000003_5.dst'

    # not much to test here
    #def test_getCacheAge(self):

    # this method is fully tested by the updateReplicaCache methods
    #def test_replicaCache(self):

    def test_LHCbDataset_cacheOutOfDate(self):
        ds = LHCbDataset()
        ds._impl.cache_date = 'Tue Jun 6 06:30:00 1944' # D-Day H-Hour
        assert ds.cacheOutOfDate(), 'cache should be out of date'
        ds._impl.cache_date = time.asctime() # now
        assert not ds.cacheOutOfDate(), 'cache should be up to date'

    # no reason to test these 2
    #def test_LHCbDataset_getMigrationClass(self):
    #def test_LHCbDataset_getMigrationObject(self):

    def test_LHCbDataset_isEmpty(self):
        ds = LHCbDataset()
        assert ds._impl.isEmpty(), 'dataset is empty'
        ds = LHCbDataset(files=[LHCbDataFile('LFN:' + self.file_name)])
        assert not ds._impl.isEmpty(), 'dataset is not empty'

    def test_LHCbDataset_updateReplicaCache(self):
        ds = LHCbDataset(files=[LHCbDataFile('LFN:' + self.file_name)])
        ds._impl.cache_date = 'Tue Jun 6 06:30:00 1944' # D-Day H-Hour
        ds.updateReplicaCache()
        assert ds._impl.files[0].replicas
        assert not ds.cacheOutOfDate()

    def test_LHCbDataFile_updateReplicaCache(self):
        df = LHCbDataFile('LFN:' + self.file_name)
        df.updateReplicaCache()
        assert df.replicas, 'replicas should now be full'
        df = LHCbDataFile('PFN:' + self.file_name)
        df.updateReplicaCache()
        assert not df.replicas, 'replicas should be empty'

    def test_LHCbDataFile__stripFileName(self):
        df = LHCbDataFile('LFN:' + self.file_name)
        ok = df._impl._stripFileName() == self.file_name
        assert ok, 'stripping should remove LFN:'

    def test_LHCbDataFile_isLFN(self):
        fname = self.file_name
        files = ['LFN:' + fname, 'Lfn:' + fname, 'lfn:' + fname]
        
        for f in files:
            df = LHCbDataFile(f)
            assert df._impl.isLFN(), '%s is LFN' % f

        df = LHCbDataFile('PFN:' + fname)
        assert not df._impl.isLFN(), '%s is NOT LFN' % 'PFN:' + fname
        
    def test_string_dataset_shortcut(self):
        files = ['LFN:' + self.file_name, 'LFN:' + self.file_name]
        ds = string_dataset_shortcut(files,None)
        assert ds, 'dataset should not be None'
        assert len(ds) == 2, 'length should be 2 not %d' % len(ds)
        
    def test_string_datafile_shortcut(self):
        file = 'LFN:' + self.file_name
        df = string_datafile_shortcut(file,None)
        assert df, 'datafile should not be None'
        assert df.name == file, 'file name should be set properly'


