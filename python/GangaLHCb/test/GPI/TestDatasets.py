from __future__ import absolute_import

import pytest
from Ganga.testlib import external
from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestDatasets(GangaUnitTest):

    # TODO: Mark as expected to fail because there should be an addProxy in __getitem__.
    # Will be superceded by new Proxy handling code from Matt Williams
    @pytest.mark.xfail
    @external
    def testDatasets(self):

        from Ganga.GPI import DiracFile, PhysicalFile, LHCbDataset, Job, LocalFile

        # test constructors/setters
        ds = LHCbDataset(['lfn:a', 'pfn:b'])
        assert len(ds) == 2
        print(ds[0])
        assert isinstance(ds[0], DiracFile)
        assert isinstance(ds[1], PhysicalFile)
        ds = LHCbDataset()
        ds.files = ['lfn:a', 'pfn:b']
        assert isinstance(ds[0], DiracFile)
        assert isinstance(ds[1], PhysicalFile)
        ds.files.append('lfn:c')
        assert isinstance(ds[-1], DiracFile)
        d = OutputData(['a', 'b'])
        assert isinstance(d.files[0],str)
        assert isinstance(d.files[1],str)

        # check job assignments
        j = Job()
        j.inputdata = ['lfn:a', 'pfn:b']
        assert isinstance(j.inputdata, LHCbDataset)
        j.outputfiles = ['a', DiracFile('b')]
        assert isinstance(j.outputfiles[0], LocalFile)
        print(type(j.outputfiles[1]))
        assert isinstance(j.outputfiles[1], DiracFile)

        LFN_DATA = ['LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00016768/0000/00016768_00000005_1.dimuon.dst',
                    'LFN:/lhcb/LHCb/Collision11/DIMUON.DST/00016768/0000/00016768_00000006_1.dimuon.dst']
        ds = LHCbDataset(LFN_DATA)

        assert len(ds.getReplicas().keys()) == 2
        assert ds.getCatalog()
