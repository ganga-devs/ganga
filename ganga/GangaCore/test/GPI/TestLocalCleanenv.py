import os
from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestLocalCleanenv(GangaUnitTest):
    def testLocalCleanenv(self):
        from GangaCore.GPI import Job
        from GangaTest.Framework.utils import sleep_until_completed, file_contains

        envname = 'LocalCleanenv_sjt5p'
        os.environ[envname] = 'Test'
        os.environ['PATH'] = os.environ['PATH'] + ':' + envname

        j = Job()

        j.submit()

        self.assertTrue(sleep_until_completed(j, 60), 'Timeout on completing job')

        self.assertEqual(j.status, 'completed')
        self.assertFalse(file_contains(j.outputdir + '/stdout', envname))
