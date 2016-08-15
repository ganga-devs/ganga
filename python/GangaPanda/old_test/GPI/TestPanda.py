from __future__ import absolute_import

from Ganga.testlib.GangaUnitTest import GangaUnitTest

class TestPanda(GangaUnitTest):

    def test_getLibFileSpecFromLibDS(self):

        from GangaPanda.Lib.Panda import getLibFileSpecFromLibDS
        fs = getLibFileSpecFromLibDS("panda.0705111709.61282.lib._8913849")

        assert fs.lfn == "panda.0705111709.61282.lib._8913849.6459655377.lib.tgz"
        assert fs.GUID == "4EA67FCA34844526B34BE9435638AB81"
        assert fs.dataset == "panda.0705111709.61282.lib._8913849"
        assert fs.destinationDBlock == "panda.0705111709.61282.lib._8913849"
        assert fs.status == "ready"
        assert fs.md5sum == "1bbb4da4"
        assert fs.fsize == "1881494"
        assert fs.scope == "panda"