from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah15771(GangaUnitTest):

    def test_CondorConfigDefaults(self):
        from Ganga.GPI import Condor, GangaList
        from Ganga.Utility.Config import setConfigOption
        from Ganga.GPIDev.Base.Proxy import isType

        setConfigOption('defaults_CondorRequirements', 'other', ['POOL == "General"'])
        setConfigOption('defaults_CondorRequirements', 'opsys', 'print')

        a = Condor()
        c = a.requirements
        assert isType(c.other, GangaList)
        assert c.opsys == 'print'
        assert c.other == ['POOL == "General"']
