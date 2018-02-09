from GangaCore.testlib.GangaUnitTest import GangaUnitTest


class TestSavannah14436(GangaUnitTest):

    def test_ConfigDefaults(self):
        from GangaCore.GPI import Executable
        from GangaCore.Utility.Config import setConfigOption

        setConfigOption('defaults_Executable', 'exe', 'echo2')
        setConfigOption('defaults_Executable', 'args', ['Hello World2'])
        a = Executable()
        assert a.exe == 'echo2'
        assert a.args == ['Hello World2']

        setConfigOption('defaults_Executable', 'exe', '/bin/echo')
        k = Executable()
        assert k.exe == '/bin/echo'
        assert k.args == ['Hello World2']
