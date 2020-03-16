from GangaCore.testlib.GangaUnitTest import GangaUnitTest
from GangaCore.Utility.feedback_report import report
from unittest import mock        

def mocked_requests_post(*args, **kwargs):
    class MockResponse:
        def __init__(self, content, status_code):
            self.content = content
            self.status_code = status_code

    path = 'http://ec2-52-14-218-28.us-east-2.compute.amazonaws.com/media/userreport2020-03-13-11:59:07.tar.gz'
    return MockResponse(("<span id=\"download_path\">path:"+path+"</span>").encode(), 200)

class TestReport(GangaUnitTest):

    @mock.patch('requests.post', side_effect=mocked_requests_post)
    def testReport(self,mock_get):
        from GangaCore.GPI import Job
        from GangaTest.Framework.utils import sleep_until_completed, file_contains
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
