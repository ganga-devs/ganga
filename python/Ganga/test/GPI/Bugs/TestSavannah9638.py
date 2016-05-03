from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest


def wipe_temp_dir():
    import os
    import shutil
    tmpdir = '/tmp/ganga_topdir-' + os.environ['USER']
    shutil.rmtree(tmpdir, ignore_errors=True)


class TestSavannah9638(GangaUnitTest):
    def setUp(self):
        super(TestSavannah9638, self).setUp()

        wipe_temp_dir()

        from Ganga.Utility.Config import setConfigOption
        setConfigOption('Configuration', 'gangadir', '/tmp/ganga_topdir-$USER')

    def test_Savannah9638(self):
        from Ganga.GPI import config, Job

        import os.path

        topdir = os.path.abspath(config['Configuration']['gangadir'])
        j = Job()
        self.assertTrue(topdir in j.inputdir)
        self.assertTrue(topdir in j.outputdir)

    def tearDown(self):
        wipe_temp_dir()

        from Ganga.Utility.Config import getConfig
        getConfig('Configuration').revertToDefault('gangadir')

        super(TestSavannah9638, self).tearDown()
