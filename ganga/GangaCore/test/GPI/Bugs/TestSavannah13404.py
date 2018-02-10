from __future__ import absolute_import

from GangaCore.testlib.GangaUnitTest import GangaUnitTest

import os


class TestSavannah13404(GangaUnitTest):

    def test_Savannah13404(self):
        from GangaCore.GPI import Job
        j = Job()
        j.submit()  # Needed in order to create the workspace
        self.assertTrue(os.path.exists(j.inputdir + '/..'))
        j.remove()
        # The job directory should be deleted
        self.assertFalse(os.path.exists(os.path.abspath(j.inputdir + '/..')))
