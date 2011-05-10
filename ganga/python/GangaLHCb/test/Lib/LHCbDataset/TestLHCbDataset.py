import time
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *

def make_dataset(files):
    ds = LHCbDataset()
    for f in files: ds.files.append(strToDataFile(f))
    return ds

class TestLHCbDataset(GangaGPITestCase):

    def test_LHCbDataset___len__(self):
        ds = LHCbDataset()
        assert len(ds) == 0
        ds = make_dataset(['pfn:a'])
        assert len(ds) == 1

    def test_LHCbDataset_isEmpty(self):
        ds = LHCbDataset()
        assert ds.isEmpty(), 'dataset is empty'
        ds = make_dataset(['pfn:a'])
        assert not ds.isEmpty(), 'dataset is not empty'

    def test_hasLFNs(self):
        ds = make_dataset(['lfn:a'])
        assert ds.hasLFNs()
        ds = make_dataset(['pfn:a'])
        assert not ds.hasLFNs()

    def test_extend(self):
        ds = make_dataset(['lfn:a'])
        ds.extend(['lfn:b'])
        assert len(ds) == 2
        ds.extend(['lfn:b'])
        assert len(ds) == 3
        ds.extend(['lfn:b'],True)
        assert len(ds) == 3

    def test_getLFNs(self):
        ds = make_dataset(['lfn:a','lfn:b','pfn:c'])
        assert len(ds.getLFNs()) == 2

    def test_getLFNs(self):
        ds = make_dataset(['lfn:a','lfn:b','pfn:c'])
        assert len(ds.getPFNs()) == 1

    def test_getFileNames(self):
        ds = make_dataset(['lfn:a','lfn:b','pfn:c'])
        assert len(ds.getFileNames()) == 3

    def test_optionsString(self):
        ds = make_dataset(['lfn:a','lfn:b','pfn:c'])
        str = ds.optionsString()
        assert str.find('LFN:a') >= 0
        assert str.find('LFN:b') >= 0
        assert str.find('PFN:') >= 0
        
    # test rest in GPI
    
