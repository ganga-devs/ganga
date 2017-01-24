from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah17080(GangaUnitTest):

    def test_CondorConfigDefaults(self):
        from Ganga.GPI import Job, TestSplitter, TestSubmitter

        j = Job()
        j.splitter = TestSplitter()
        j.splitter.backs = [TestSubmitter(),TestSubmitter(),TestSubmitter()]
        j.backend= TestSubmitter()
        b = j.splitter.backs[1]
        b.fail = 'submit'

        assert j.status == 'new'

        j.submit(keep_going=False)
        assert j.status in ['submitted', 'running']
        assert j.subjobs[0].status in ['submitted', 'running']

        assert j.subjobs[1].status == 'new'
        assert j.subjobs[2].status == 'new'
