

from GangaCore.testlib.mark import external
from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestDatasets(GangaUnitTest):

    def testDatasetsFunctions(self):

        from GangaCore.GPI import DiracFile, LHCbDataset, Job, LocalFile

        # test constructors/setters
        ds = LHCbDataset(['lfn:a', 'pfn:b'])
        assert len(ds) == 2
        print((ds[0]))
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
        print((type(j.outputfiles[1])))
        assert isinstance(j.outputfiles[1], DiracFile)

        # check the LHCbDataset functions:

        assert ds.getLFNs() == ['a', 'c']
        assert ds.getPFNs() == ['b']

        # check the file types are preserved
        assert isinstance(ds.difference(ds2)[0], LocalFile)
        assert isinstance(ds.symmetricDifference(ds2)[0], LocalFile)
        assert isinstance(ds2.difference(ds)[0], DiracFile)
        assert isinstance(ds2.symmetricDifference(ds)[0], LocalFile)
        assert isinstance(ds2.union(ds)[0], DiracFile)
        assert isinstance(ds2.union(ds)[1], LocalFile)
        assert isinstance(ds2.intersection(ds)[0], DiracFile)

        ds2 = LHCbDataset(['lfn:a', 'lfn:d'])
        ds.extend(ds2, True)
        assert len(ds) == 4

        # check the useful difference functions etc
        assert sorted(ds.difference(ds2).getFileNames()) == ['b', 'c']
        assert sorted(ds.symmetricDifference(ds2).getFileNames()) == ['b', 'c']
        assert sorted(ds.intersection(ds2).getFileNames()) == ['a', 'd']
        assert sorted(ds.union(ds2).getFileNames()) == ['a', 'b', 'c', 'd']

    @external
    def testDatasets(self):

        from GangaCore.GPI import LHCbDataset

        # test behaviour with files on the grid

        LFN_DATA = ['LFN:/lhcb/LHCb/Collision17/DIMUON.DST/00067804/0003/00067804_00030224_1.dimuon.dst',
                    'LFN:/lhcb/LHCb/Collision17/DIMUON.DST/00067804/0003/00067804_00030520_1.dimuon.dst']

        ds = LHCbDataset(LFN_DATA)

        assert len(list(ds.getReplicas().keys())) == 2
        assert ds.getCatalog()

    def testCompressedDatasetsFunctions(self):

        from GangaCore.GPI import DiracFile, LHCbCompressedDataset, Job

        # test constructors/setters
        #First with a list of lfns
        ds = LHCbCompressedDataset(['/path/to/some/file', '/path/to/some/otherfile'])
        assert len(ds) == 2

        #Check the indexing and slicing - the __getitem__ and __len__ methods
        ds1 = LHCbCompressedDataset(['/first/path/set/a', '/first/path/set/b'])
        ds2 = LHCbCompressedDataset(['/second/path/set/c', '/second/path/set/d'])
        ds1.extend(ds2)
        assert isinstance(ds1[0], DiracFile)
        assert ds1[0].lfn == '/first/path/set/a'
        assert isinstance(ds1[2], DiracFile)
        assert ds1[2].lfn == '/second/path/set/c'
        assert isinstance(ds1[0:2], LHCbCompressedDataset)
        assert len(ds1[0:2]) == 2
        assert len(ds1[0:2].files) == 1
        assert len(ds1[::2]) == 2

        #Check the getLFNs
        assert ds1.getLFNs() == ['/first/path/set/a', '/first/path/set/b', '/second/path/set/c', '/second/path/set/d']

        #Check extend, union, subset superset, difference
        ds1 = LHCbCompressedDataset(['/path/to/some/file/a', '/path/to/some/otherfile/b'])
        ds2 = LHCbCompressedDataset(['/otherpath/to/some/file/c', '/path/to/some/otherfile/d'])
        ds3 = ds1.union(ds2)
        assert len(ds3) == 4
        assert sorted(ds3.getLFNs()) == ['/otherpath/to/some/file/c', '/path/to/some/file/a', '/path/to/some/otherfile/b', '/path/to/some/otherfile/d']
        assert(ds1.isSubset(ds3))
        assert(ds3.isSuperset(ds2))

        ds4 = ds3.difference(ds1)
        assert sorted(ds4.getLFNs()) == ['/otherpath/to/some/file/c', '/path/to/some/otherfile/d']

        ds1.extend(ds2)
        assert len(ds1) == 4
        assert ds1.getLFNs() == ['/path/to/some/file/a', '/path/to/some/otherfile/b', '/otherpath/to/some/file/c', '/path/to/some/otherfile/d']


