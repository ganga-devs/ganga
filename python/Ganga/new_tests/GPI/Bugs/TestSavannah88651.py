from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


class TestSavannah88651(GangaUnitTest):
    def test_Savannah88651(self):
        from Ganga.GPI import Job, config

        # Feature request 88651 was to allow the behaviour of copying a prepared job to be configurable.
        # Specifically, the user wanted to be able to control whether the job/application was unprepared upon copying.

        # test 1: do not unprepare the job when copying
        a = Job()
        a.prepare()
        config['Preparable']['unprepare_on_copy'] = False
        b = a.copy()

        self.assertNotEqual(b.application.is_prepared, None)

        config['Preparable']['unprepare_on_copy'] = True
        b = a.copy()
        # b's application should now be unprepared when copied from a.
        self.assertEqual(b.application.is_prepared, None)
