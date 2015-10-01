from GangaTest.Framework.tests import GangaGPITestCase
from Ganga.Utility.Config import getConfig, ConfigError

from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *

from GangaDirac.Lib.Files.DiracFile import DiracFile

class TestLHCbDatasetUtils(GangaGPITestCase):

    def test_isLFN(self):
        from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import isLFN
        DiracFile('test')
        isLFN(DiracFile('test'))
        assert isLFN(DiracFile('test')), 'should be true'
        assert not isLFN(PhysicalFile('test')), 'should be false'

    def test_isPFN(self):
        from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import isPFN
        assert isPFN(PhysicalFile('test')), 'should be true'
        assert not isPFN(DiracFile('test')), 'should be false'

    def test_strToDataFile(self):
        from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import strToDataFile
        #assert isinstance(strToDataFile('pfn:a'), PhysicalFile)
        assert isinstance(strToDataFile('lfn:a'), DiracFile)
        assert strToDataFile('a') is None

    def test_getDataFile(self):
        from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import getDataFile
        lfn = DiracFile('a')
        pfn = LocalFile('a')
        assert getDataFile(lfn) == lfn
        assert getDataFile(pfn) == pfn
        assert getDataFile('lfn:a') == strToDataFile('lfn:a')
        #assert getDataFile('pfn:a') == strToDataFile('pfn:a')
