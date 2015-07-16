
from GangaUnitTest import GangaUnitTest

class Savannah10064(GangaUnitTest):

    def testSavannah10064(self):
        from Ganga.GPI import Job, TestSubmitter, TestApplication
        j = Job(backend=TestSubmitter(),application=TestApplication())
        j.submit()
        import os.path
        assert(os.path.exists(j.inputdir))
        templates.remove()
        assert(os.path.exists(j.inputdir))

