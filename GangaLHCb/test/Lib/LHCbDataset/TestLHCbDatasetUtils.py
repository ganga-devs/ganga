from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
from Ganga.Utility.Config import getConfig, ConfigError

class TestLHCbDatasetUtils(GangaGPITestCase):

    def test_isLFN(self):
        LogicalFile('test')
        isLFN(LogicalFile('test'))
        assert isLFN(LogicalFile('test')), 'should be true'
        assert not isLFN(PhysicalFile('test')), 'should be false'

    def test_isPFN(self):        
        assert isPFN(PhysicalFile('test')), 'should be true'
        assert not isPFN(LogicalFile('test')), 'should be false'

    def test_strToDataFile(self):
        assert isinstance(strToDataFile('pfn:a'),PhysicalFile)
        assert isinstance(strToDataFile('lfn:a'),LogicalFile)
        assert strToDataFile('a') is None

    def test_getDataFile(self):
        lfn = LogicalFile('a')
        pfn = LogicalFile('a')
        assert getDataFile(lfn) == lfn
        assert getDataFile(pfn) == pfn
        assert getDataFile('lfn:a') == strToDataFile('lfn:a')
        assert getDataFile('pfn:a') == strToDataFile('pfn:a')
    

    
