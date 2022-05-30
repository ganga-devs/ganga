import os
import unittest

from ganga.GangaCore.testlib.GangaUnitTest import GangaUnitTest
from ganga.GangaCore.testlib.monitoring import run_until_completed
from GangaCore.Utility.feedback_report import report

# This file tests the 'report' utlity which outputs debugging info.


class TestFeedbackReport(GangaUnitTest):
    def test_report_local(self):
        ''' Test that the report function produces a non-empty LocalFile output by default '''
        from GangaCore.GPI import Job, LocalFile

        j = Job()
        j.submit()
        self.assertTrue(run_until_completed(j))

        report_result = report(j)
        self.assertTrue(isinstance(report_result, LocalFile))

        outputfile = os.path.join(report_result.localDir, report_result.namePattern)
        self.assertTrue(os.path.exists(outputfile))
        self.assertGreater(os.path.getsize(outputfile), 0)

        if (os.path.exists(outputfile)):
            os.remove(outputfile)


if __name__ == "__main__":
    unittest.main()
