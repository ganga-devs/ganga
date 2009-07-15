from GangaTest.Framework.tests import GangaGPITestCase

class TestLHCbDataset(GangaGPITestCase):

    #lfn data for test
    LFN_DATA = ["LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000001_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000002_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000003_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000004_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000005_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000006_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000007_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000008_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000009_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000010_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000011_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000012_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000013_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000014_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000015_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000016_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000017_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000018_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000019_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000020_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000022_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000023_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000024_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000025_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000026_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000027_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000028_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000029_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000030_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000031_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000032_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000033_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000034_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000035_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000036_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000037_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000038_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000039_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000041_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000042_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000043_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000045_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000048_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000049_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000050_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000052_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000053_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000055_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000056_5.dst",
                "LFN:/lhcb/production/DC06/phys-v2-lumi2/00001657/DST/0000/00001657_00000057_5.dst"]

    def __init__(self):
        self.data_set = None

    def setUp(self):

        self.data_set = LHCbDataset(files = [LHCbDataFile(f) for f in self.LFN_DATA])

    def testSimpleUpdateCacheDate(self):

        date = self.data_set.cache_date

        self.data_set.updateReplicaCache();
        assert self.data_set.cache_date != date, 'The cache date should have been updated'


    def testForcedUpdateCacheDate(self):

        self.data_set.updateReplicaCache();
        date = self.data_set.cache_date

        #force at least one file to need updating
        self.data_set.files[0].replicas = []
        self.data_set.updateReplicaCache(forceUpdate = True);

        assert self.data_set.cache_date != date, 'The cache date should have been updated'

    def testCacheValidDefault(self):

        self.data_set.updateReplicaCache();
        assert not self.data_set.cacheOutOfDate(), 'Cache should be valid'

    def testCacheValidWait(self):
        
        self.data_set.updateReplicaCache();

        import time
        from GangaLHCb.Lib.LHCbDataset import getCacheAge
        time.sleep( (getCacheAge()*60) + 10)
        
        assert self.data_set.cacheOutOfDate(), 'Cache should be out of date'

    def testCacheValidWaitFlag(self):
        
        self.data_set.updateReplicaCache();

        cache_age = 2
        assert not self.data_set.cacheOutOfDate(maximum_cache_age = cache_age), 'Cache should be valid'

        import time
        time.sleep( (cache_age*60) + 10)
        
        assert self.data_set.cacheOutOfDate(maximum_cache_age = cache_age), 'Cache should be out of date'

    def testNegativeOrZeroCacheAgesShouldFail(self):

        assert self.data_set.cacheOutOfDate(maximum_cache_age = 0), 'Cache should not be valid'
        assert self.data_set.cacheOutOfDate(maximum_cache_age = -1), 'Cache should not be valid'

    def testLen(self):

        assert len(self.data_set) == len(self.LFN_DATA), 'Length should be correctly reported'
        
    def testTruth(self):
        
        assert bool(self.data_set) == bool(self.LFN_DATA), 'Truth symantics should be of list'
        assert bool(self.data_set.files) == bool(self.LFN_DATA), 'Truth symantics should be of list'

        ds = LHCbDataset()

        assert bool(ds) == bool([]), 'Truth symantics should be of list'
        assert bool(ds.files) == bool([]), 'Truth symantics should be of list'
        
        assert ds._impl, 'Datasets should always be True'
        assert self.data_set._impl, 'Datasets should always be True'
        
    def testJobCopy(self):
        
        j = Job(inputdata = self.data_set)
        assert j.inputdata._impl.new_cache, 'Cache should be new' 
        j.inputdata.updateReplicaCache(forceUpdate = True)
        assert len(j.inputdata) == len(self.LFN_DATA), 'Length should be correctly reported'
        assert not j.inputdata._impl.new_cache, 'Cache should be updated' 
        
        j2 = j.copy()
        assert not j.inputdata._impl.new_cache, 'Cache 2 should be dirty' 
        assert len(j2.inputdata) == len(self.LFN_DATA), 'Length should be correctly reported'
        
        for i in xrange(len(j.inputdata.files)):
            assert j.inputdata.files[i].name == j2.inputdata.files[i].name
            assert j.inputdata.files[i].replicas == j2.inputdata.files[i].replicas
        assert not j2.inputdata.cacheOutOfDate(maximum_cache_age = 10)
        
        cache_date = j2.inputdata.cache_date
        assert cache_date == j.inputdata.cache_date, 'Cache dates should be the same'
        j2.inputdata.updateReplicaCache(forceUpdate = False)
        assert cache_date == j2.inputdata.cache_date, 'Cache dates should be the same'
        j2.inputdata.updateReplicaCache(forceUpdate = True)
        assert cache_date != j2.inputdata.cache_date, 'Cache dates should not be the same'
        assert cache_date == j.inputdata.cache_date, 'Cache dates should be the same'
