

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
        lfn = DiracFile(lfn='/lhcb/LHCb/Collision16/DIMUON.DST/00053485/0000/00053485_00000424_1.dimuon.dst')
        assert lfn.getReplicas()
        assert lfn.getMetadata()
