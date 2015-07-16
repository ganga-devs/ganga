
from GangaUnitTest import GangaUnitTest

class Savannah13459(GangaUnitTest):

    def testSavannah13459(self):
        from Ganga.GPI import Job, Executable
        j = Job()
        j.application = Executable()
        j.application.args = ['1','2','3']
        assert(j.application.args == ['1','2','3'])
        j.application.args[0]='0'

        assert(j.application.args == ['0','2','3'])

