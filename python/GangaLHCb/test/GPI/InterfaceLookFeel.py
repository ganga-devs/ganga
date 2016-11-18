from __future__ import absolute_import

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestInterfaceLookFeel(GangaUnitTest):

    def testInterfaceLookFeel(self):
        """
        This test tests the Executable app and that the DaVinci are assignable
        """
        from Ganga.GPI import Job, LSF, Executable, DaVinci

        j1 = Job(name='my',application='DaVinci')
        j2 = Job(application = DaVinci())

        j1.backend = LSF()
        j1.backend.queue = '8nm'
        j2.backend = j1.backend # deepcopy
        j2.backend.queue = '16nh' # shortcut
        bk2 = j2.backend # reference

        assert j2.backend.queue == '16nh'
        bk2.queue = '100nh'
        assert j2.backend.queue == '100nh'

        ap = Executable()

        j1.application = ap # deepcopy
