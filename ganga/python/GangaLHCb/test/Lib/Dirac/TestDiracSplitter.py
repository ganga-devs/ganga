import os
from GangaTest.Framework.tests import GangaGPITestCase
from GangaLHCb.Lib.LHCbDataset import LHCbDataFile
from GangaLHCb.Lib.Dirac.DiracSplitter import _diracSplitter

class TestDiracSplitter(GangaGPITestCase):

    # this is just a wrapper for _diracSplitter.split
    #def test_DiracSplitter__splitFiles(self):

    def test__diracSplitter__copyToDataSet(self):
        data = LHCbDataFile()
        data.name = 'dummy.file'
        repmap = {'dummy.file' : 1}
        ds = _diracSplitter(1,1,True)
        dataset = ds._copyToDataSet([data],repmap)
        assert len(dataset) == 1, 'incorrect length'
        

    # test this in the GPI tests
    #def test__diracSplitter_split(self):

    
