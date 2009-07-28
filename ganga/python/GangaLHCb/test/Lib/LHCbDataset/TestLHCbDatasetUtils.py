from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
from Ganga.Utility.Config import getConfig, ConfigError

class TestLHCbDatasetUtils(GangaGPITestCase):

    def setUp(self):
        pass

    def test_getCacheAge(self):
        config = getConfig('LHCb')
        config.setUserValue('maximum_cache_age',66)
        assert getCacheAge() == 66
        config.setUserValue('maximum_cache_age',-1)
        assert getCacheAge() > 0

    # this method fully tested in LHCbDataset::updateReplicaCache
    #def test_replicaCache(self):

    def test_collect_lhcb_filelist(self):
        l = collect_lhcb_filelist(['file1','file2','file3'])
        assert len(l) == 3, 'collect incorrect number of files from list'
        ds = LHCbDataset(files=['file1','file2','file3'])
        d = collect_lhcb_filelist(ds)
        good = (len(d) == len(ds.files))
        assert good, 'collect incorrect number of files from dataset'

    def test_dataset_to_options_string(self):
        s = dataset_to_options_string(None)
        assert s == '', 'None should return an empty string'
        s = dataset_to_options_string(LHCbDataset(['a','b','c']))
        assert s != '', 'dataset should not return an empty string'
    
    def test_dataset_to_lfn_string(self):
        s = dataset_to_lfn_string(None)
        assert s == '', 'None should return an empty string'
        s = dataset_to_lfn_string(LHCbDataset(['a','b','c']))
        assert s == '', 'no lfns should return empty string'
        s = dataset_to_lfn_string(LHCbDataset(['lfn:a','lfn:b','lfn:c']))
        assert s != '', 'lfns should not return empty string'
    
