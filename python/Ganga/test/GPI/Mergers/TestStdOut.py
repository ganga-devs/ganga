from __future__ import division, absolute_import

import os
import tempfile

from ..GangaUnitTest import GangaUnitTest


class TestStdOut(GangaUnitTest):
    def setUp(self):
        super(TestStdOut, self).setUp()
        from Ganga.GPI import Job, Executable, Local, File, LocalFile, config
        from GangaTest.Framework.utils import write_file

        config['Mergers']['associate'] = {'stdout': 'RootMerger'}

        self.jobslice = []
        self.file_name = 'id_echo.sh'

        for i in range(2):

            j = Job(application=Executable(), backend=Local())

            scriptString = '''
            #!/bin/sh
            echo "Output from job $1." > out.txt
            echo "Output from job $2." > out2.txt
            '''

            # write string to tmpfile
            tmpdir = tempfile.mktemp()
            os.mkdir(tmpdir)
            fileName = os.path.join(tmpdir, self.file_name)

            write_file(fileName, scriptString)

            j.application.exe = 'sh'
            j.application.args = [File(fileName), str(j.id), str(j.id * 10)]
            j.outputfiles = [LocalFile('out.txt'), LocalFile('out2.txt')]
            self.jobslice.append(j)

    def runJobSlice(self):
        from GangaTest.Framework.utils import sleep_until_completed

        for j in self.jobslice:
            j.submit()
            assert sleep_until_completed(j, timeout=60), 'Timeout on job submission: job is still not finished'
            assert j.status == 'completed'

    def testCanSetStdOutMerge(self):
        from Ganga.GPI import SmartMerger

        from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException

        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        sm = SmartMerger()
        sm.files = ['stdout']
        try:
            assert not sm.merge(self.jobslice, tmpdir)
        except PostProcessException:
            pass
