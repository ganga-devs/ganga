from __future__ import absolute_import

from Ganga.new_tests.lib.GangaUnitTest import GangaUnitTest


class TestArgSplitter(GangaUnitTest):
    def TestArgSplitter(self):
        from Ganga.GPI import Job, ArgSplitter
        from GangaTest.Framework.utils import sleep_until_completed

        j = Job()
        j.splitter = ArgSplitter(args=[['1'], ['2'], ['3']])
        j.submit()

        self.assertTrue(sleep_until_completed(j, 60), 'Timeout on completing job')

        self.assertEqual(len(j.subjobs), 3)
