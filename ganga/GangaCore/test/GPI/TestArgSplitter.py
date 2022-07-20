from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestArgSplitter(GangaUnitTest):
    def testArgSplitter(self):
        from GangaCore.GPI import Job, ArgSplitter
        from GangaTest.Framework.utils import sleep_until_completed, file_contains

        j = Job()
        j.splitter = ArgSplitter(args=[['1'], ['2'], ['3']])

        self.assertTrue(not j.subjobs)
        self.assertEqual(len(j.subjobs), 0)

        j.submit()

        self.assertEqual(len(j.subjobs), 3)

        for s in j.subjobs:
            self.assertEqual(s.master, j)
            self.assertIn(s.status, ['submitted', 'running', 'completed'])

        self.assertTrue(sleep_until_completed(j, 60), 'Timeout on completing job')

        for i, s in zip(range(len(j.subjobs)), j.subjobs):
            self.assertEqual(s.status, 'completed')
            self.assertTrue(file_contains(s.outputdir + '/stdout', '%d' % (i + 1)))
