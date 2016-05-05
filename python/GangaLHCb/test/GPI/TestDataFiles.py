from __future__ import absolute_import

from Ganga.testlib import external
from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestDataFiles(GangaUnitTest):

    @external
    def testDataFiles(self):

        from Ganga.GPI import DiracFile, config

        print config['LHCb']
        # LFNs
        name = 'test.txt'
        lfn = DiracFile(lfn=name)
        assert lfn.lfn == name
        lfn = DiracFile('lfn:'+name)
        assert lfn.lfn == name
        lfn = DiracFile('LFN:'+name)
        assert lfn.lfn == name
        try:
            lfn = DiracFile('pfn:'+name)
            raise RuntimeError('Should have got exception trying to create DiracFile from pfn.')
        except:
            pass

        # Methods
        lfn = DiracFile(lfn='/lhcb/data/2010/DIMUON.DST/00008395/0000/00008395_00000326_1.dimuon.dst')
        assert lfn.getReplicas()
        assert lfn.getMetadata()
