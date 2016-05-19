from __future__ import absolute_import

from Ganga.testlib.mark import external
from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestDataFiles(GangaUnitTest):

    def testDataFiles(self):

        from Ganga.GPI import DiracFile, config

        # LFNs
        name = 'test.txt'
        lfn = DiracFile(lfn=name)
        assert lfn.lfn == name
        lfn = DiracFile('lfn:'+name)
        assert lfn.lfn == name
        lfn = DiracFile('LFN:'+name)
        assert lfn.lfn == name

        # not sure if this should raise an exception or not. The original test had a try..except that would always
        # pass so not sure :)
        lfn = DiracFile('pfn:'+name)


    @external
    def testDataFilesExternal(self):
        from Ganga.GPI import DiracFile
        
        # Methods
        lfn = DiracFile(lfn='/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000326_1.dimuon.dst')
        assert lfn.getReplicas()
        assert lfn.getMetadata()
