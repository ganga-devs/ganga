from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah14436(GangaUnitTest):

    def test_ConfigDefaults(self):
        from Ganga.GPI import Job
        from Ganga.Utility.Config import setConfigOption

        setConfigOption('defaults_Executable', 'exe', 'echo2')
        setConfigOption('defaults_Executable', 'args', ['Hello World2'])
        j = Job()
        assert j.application.exe == 'echo2'
        assert j.application.args == ['Hello World2']

        setConfigOption('defaults_Executable', 'exe', '/bin/echo')
        k = Job()
        assert k.application.exe == '/bin/echo'
        assert k.application.args == ['Hello World2']
