from __future__ import absolute_import

import pytest
from Ganga.testlib.mark import external
from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestDatasets(GangaUnitTest):

    def testDatasetsFunctions(self):

        from Ganga.GPI import DiracFile, PhysicalFile, LHCbDataset, Job, LocalFile

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
#        d = OutputData(['a', 'b'])
#        assert isinstance(d.files[0],str)
#        assert isinstance(d.files[1],str)

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




    # TODO: Mark as expected to fail because there should be an addProxy in __getitem__.
    # Will be superceded by new Proxy handling code from Matt Williams
#    @pytest.mark.xfail
    @external
    def testDatasets(self):

        from Ganga.GPI import DiracFile, PhysicalFile, LHCbDataset, Job, LocalFile

        #test behaviour with files on the grid

        LFN_DATA = ['LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00016768/0000/00016768_00000005_1.dimuon.dst',
                    'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00016768/0000/00016768_00000006_1.dimuon.dst']
        ds = LHCbDataset(LFN_DATA)

        assert len(ds.getReplicas().keys()) == 2
        assert ds.getCatalog()
