from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Utility.feedback_report import report

# Test Report() Functionality
class TestReport(GangaUnitTest):


    def test_report(self):
        from GangaCore.GPI import Job, LocalFile
        from GangaTest.Framework.utils import sleep_until_completed
        import os

        j = Job()
        
        # Assert pre-submission job information
        self.assertTrue(not j.subjobs)
        self.assertEqual(len(j.subjobs), 0)

        j.submit()

        # Assert post-submission job information
        self.assertEqual(len(j.subjobs), 0)
        self.assertIn(j.status, ['submitted','running','completed'])  
        
        # Create a report and store as LocalFile 
        r = report(j, filetype=LocalFile)

        # Check if the created file by report() exists
        self.assertTrue(os.path.exists(os.path.join(r.localDir, r.namePattern)))
