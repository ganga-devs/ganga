from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestInterfaceLookFeel(GangaUnitTest):

    def testInterfaceLookFeel(self):

        # Just testing that the job construction works

        from Ganga.GPI import Job, Im3ShapeApp, Im3ShapeSplitter, DiracFile, LocalFile, GangaDataset, Dirac

        j = Job()
        app = Im3ShapeApp(
            im3_location=DiracFile(lfn='/lsst/y1a1-v2-z/software/2016-02-24/im3shape-grid.tar.gz'),
            ini_location=LocalFile('/afs/cern.ch/user/r/rcurrie/cmtuser/GANGA/GANGA_LSST/install/ganga/python/params_disc.ini'),
            blacklist=LocalFile('/afs/cern.ch/user/r/rcurrie/cmtuser/GANGA/GANGA_LSST/install/ganga/python/blacklist-y1.txt')
        )
        j.application = app
        j.backend = Dirac()
        mydata = GangaDataset()
        mydata.append(DiracFile(lfn='/lsst/DES0005+0043-z-meds-y1a1-gamma.fits.fz'))
        j.inputdata = mydata
        j.splitter = Im3ShapeSplitter(size=20)
        j.outputfiles = [DiracFile('*.main.txt'), DiracFile('*.epoch.txt')]
