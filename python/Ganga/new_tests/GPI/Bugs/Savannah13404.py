import pytf.lib
import os
from GangaUnitTest import GangaUnitTest


class Savannah13404(GangaUnitTest):

    def testSavannah13404(self):
        from Ganga.GPI import Job
        j = Job()
        assert(os.path.exists(j.inputdir + '/..'))
        j.remove()
        # The job directory should be deleted
        assert(not os.path.exists(os.path.abspath(j.inputdir + '/..')))
