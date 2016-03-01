from __future__ import division, absolute_import

import os
import tempfile

import pytest

from ..GangaUnitTest import GangaUnitTest
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException


class TestStructure(GangaUnitTest):

    def setUp(self):
        super(TestStructure, self).setUp()
        from Ganga.GPI import Job, Executable, Local, File, LocalFile
        from GangaTest.Framework.utils import write_file

        self.jobslice = []
        self.file_name = 'id_echo.sh'

        for _ in range(5):

            j = Job(application=Executable(), backend=Local())

            scriptString = '''
            #!/bin/sh
            echo "Output from job $1." > out.txt
            mkdir -p subdir
            echo "Output from job $2." > subdir/out.txt
            '''

            # write string to tmpfile
            tmpdir = tempfile.mktemp()
            os.mkdir(tmpdir)
            fileName = os.path.join(tmpdir, self.file_name)

            write_file(fileName, scriptString)

            j.application.exe = 'sh'
            j.application.args = [File(fileName), str(j.id), str(j.id * 10)]
            j.outputfiles = [LocalFile('out.txt'), LocalFile('subdir/out.txt')]
            self.jobslice.append(j)

    def runJobSlice(self):
        from GangaTest.Framework.utils import sleep_until_completed

        for j in self.jobslice:
            j.submit()
        for j in self.jobslice:
            assert sleep_until_completed(j, timeout=10, verbose=True), 'Timeout on job submission: job is still not finished'
            print('# upcoming status')
            print("Status 3: %s" % j.status)
            print('# printed status')
            print('# upcoming impl status')
            print("Status 3: %s" % j._impl.status)
            print('# printed impl status')
            print 'j._impl.__dict__', j._impl.__dict__
            #print 'type(j).__dict__', type(j).__dict__
            assert j.status == 'completed'

    def testStructureCreated(self):

        self.runJobSlice()

        for j in self.jobslice:
            assert os.path.exists(os.path.join(j.outputdir, 'out.txt')), 'File must exist'
            assert os.path.exists(os.path.join(j.outputdir, 'subdir', 'out.txt')), 'File in directory must exist'

    def testStructureOfMerger(self):
        """Test that structure in the output sandbox is recreated in the merge"""
        from Ganga.GPI import TextMerger
        self.runJobSlice()

        tm = TextMerger()
        tm.files = ['out.txt', 'subdir/out.txt']

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        assert tm.merge(self.jobslice, tmpdir), 'Merge must run correctly'
        assert os.path.exists(os.path.join(tmpdir, 'out.txt')), 'Merge must produce the file'

    def testOutputEqualsInput(self):
        """Tests that setting outputdir == inputdir fails always"""
        from Ganga.GPI import TextMerger

        self.runJobSlice()
        tm = TextMerger(overwrite=True)
        tm.files = ['out.txt']

        for j in self.jobslice:
            with pytest.raises(PostProcessException):
                tm.merge(self.jobslice, outputdir=j.outputdir)
