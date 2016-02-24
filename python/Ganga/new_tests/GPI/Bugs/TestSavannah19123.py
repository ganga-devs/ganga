from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

import os


class TestSavannah19123(GangaUnitTest):
    def test_Savannah19123(self):
        from Ganga.GPI import Job, Local

        from GangaTest.Framework.utils import sleep_until_state

        # check if stdout and stderr exists or not, flag indicates if files are required to exist or not

        def check(exists_flag):
            for fn in ['stdout','stderr']:
                fn = os.path.join(j.outputdir,fn)
                file_exists = os.path.exists(fn)
                if exists_flag:
                    self.assertTrue(file_exists, 'file %s should exist but it does not' % fn)
                else:
                    self.assertFalse(file_exists, 'file %s should not exist but it does' % fn)

        j = Job()
        j.application.exe = 'bash'
        j.application.args = ['-c', 'for i in `seq 1 30`; do echo $i; sleep 1; done']
        j.backend = Local()

        j.submit()

        check(False)

        sleep_until_state(j, 5, 'running')

        if j.status not in ['failed', 'completed']:
            j.kill()

        check(True)
