from GangaUnitTest import GangaUnitTest

class TestJob(GangaUnitTest):

    def testJobCreate(self):
        from Ganga.GPI import Job
        j = Job()
