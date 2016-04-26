from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah47814(GangaUnitTest):
    def test_Savannah47814(self):
        from Ganga.GPI import Job, Executable

        from GangaTest.Framework.utils import sleep_until_state, file_contains

        j = Job()
        j.application = Executable(exe='ThisScriptDoesNotExist')
        j.submit()

        failed = sleep_until_state(j, 60, state='failed', break_states=['new', 'killed', 'completed', 'unknown', 'removed'])
        self.assertTrue(failed, 'Job with illegal script should fail. Instead it went into the state %s' % j.status)

        import os.path
        f = os.path.join(j.outputdir, '__jobstatus__')
        self.assertTrue(file_contains(f, 'No such file or directory'), '__jobstatus__ file should contain error')
