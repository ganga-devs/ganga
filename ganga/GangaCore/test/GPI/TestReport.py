from GangaCore.testlib.GangaUnitTest import GangaUnitTest

class TestReport(GangaUnitTest):
    def testReport(self):
        from GangaCore.GPI import Job
        from GangaTest.Framework.utils import sleep_until_completed, file_contains
        from GangaCore.Utility.feedback_report import report
        import os
        j = Job()
        
        self.assertTrue(not j.subjobs)
        self.assertEqual(len(j.subjobs), 0)

        j.submit()

        self.assertEqual(len(j.subjobs), 0)

        self.assertIn(j.status, ['submitted','running','completed'])  
        self.assertTrue(sleep_until_completed(j, 60), 'Timeout on completing job')
        
        response,file_path = report(j)
        self.assertTrue(os.path.exists(file_path))
        self.assertEqual(response.status_code, 200)
