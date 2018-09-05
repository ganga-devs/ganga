from __future__ import absolute_import

import pytest
from GangaCore.testlib.mark import external
from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestDatasets(GangaUnitTest):

    def testDatasetsFunctions(self):

        from GangaCore.GPI import DiracFile, PhysicalFile, LHCbDataset, Job, LocalFile

        # test constructors/setters
        ds = LHCbDataset(['lfn:a', 'pfn:b'])
        assert len(ds) == 2
        print(ds[0])
        assert isinstance(ds[0], DiracFile)
        assert isinstance(ds[1], LocalFile)
        ds = LHCbDataset()
        ds.files = ['lfn:a', 'pfn:b']
        assert isinstance(ds[0], DiracFile)
        assert isinstance(ds[1], LocalFile)
        assert ds.getFullFileNames() == ['LFN:a', 'PFN:b']
        ds.files.append('lfn:c')
        assert isinstance(ds[-1], DiracFile)

        # check job assignments
        j = Job()
        j.inputdata = ['lfn:a', 'pfn:b']
        assert isinstance(j.inputdata, LHCbDataset)
        j.outputfiles = ['a', DiracFile('b')]
        assert isinstance(j.outputfiles[0], LocalFile)
        print(type(j.outputfiles[1]))
        assert isinstance(j.outputfiles[1], DiracFile)

        # check the LHCbDataset functions:

        assert ds.getLFNs() == ['a','c']
        assert ds.getPFNs() == ['b']

        ds2 = LHCbDataset(['lfn:a', 'lfn:d'])
        ds.extend(ds2, True)
        assert len(ds) == 4

        # check the useful difference functions etc
        assert sorted(ds.difference(ds2).getFileNames()) == ['b','c']
        assert sorted(ds.symmetricDifference(ds2).getFileNames()) == ['b','c']
        assert sorted(ds.intersection(ds2).getFileNames()) == ['a', 'd']
        assert sorted(ds.union(ds2).getFileNames()) == ['a', 'b', 'c', 'd']

    @external
    def testDatasets(self):

        from GangaCore.GPI import DiracFile, PhysicalFile, LHCbDataset, Job, LocalFile

        #test behaviour with files on the grid

        LFN_DATA = ['LFN:/lhcb/LHCb/Collision17/DIMUON.DST/00067804/0003/00067804_00030224_1.dimuon.dst', 'LFN:/lhcb/LHCb/Collision17/DIMUON.DST/00067804/0003/00067804_00030520_1.dimuon.dst']

        ds = LHCbDataset(LFN_DATA)

        assert len(ds.getReplicas().keys()) == 2
        assert ds.getCatalog()
