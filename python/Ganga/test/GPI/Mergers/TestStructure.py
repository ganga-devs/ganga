from __future__ import division, absolute_import

import os
import tempfile

import pytest

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.testlib.monitoring import run_until_completed
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException


class TestStructure(GangaUnitTest):

    def setUp(self):
        super(TestStructure, self).setUp()
        from Ganga.GPI import Job, Executable, Local, File, LocalFile
        from GangaTest.Framework.utils import write_file

        self.jobslice = []
        self.file_name = 'id_echo.sh'

        self.subdir_path = 'subdir'
        self.outputfile = 'out.txt'

        for _ in range(5):

            j = Job(application=Executable(), backend=Local())

            scriptString = '''
            #!/bin/sh
            echo "Output from job $1." > %s
            mkdir -p subdir
            echo "Output from job $2." > %s
            ''' % (self.outputfile, os.path.join(self.subdir_path, self.outputfile))

            # write string to tmpfile
            tmpdir = tempfile.mktemp()
            os.mkdir(tmpdir)
            fileName = os.path.join(tmpdir, self.file_name)

            write_file(fileName, scriptString)

            j.application.exe = 'sh'
            j.application.args = [File(fileName), str(j.id), str(j.id * 10)]
            j.outputfiles = [LocalFile(self.outputfile), LocalFile(os.path.join(self.subdir_path, self.outputfile))]
            self.jobslice.append(j)

            assert len(j.outputfiles) == 2

    def runJobSlice(self):

        for j in self.jobslice:
            this_bak = j.backend

        for j in self.jobslice:
            j.submit()
        for j in self.jobslice:
            assert run_until_completed(j, timeout=10), 'Timeout on job submission: job is still not finished'
            #print('# upcoming status')
            #print("Status 3: %s" % j.status)
            #print('# printed status')
            #print('# upcoming impl status')
            #print("Status 3: %s" % j._impl.status)
            #print('# printed impl status')
            #print('j._impl.__dict__: %s' j._impl.__dict__)
            #print 'type(j).__dict__', type(j).__dict__

    def testStructureCreated(self):

        self.runJobSlice()

        for j in self.jobslice:
            print("Examining: %s" % j.id)
            print("status: %s" % j.status)
            print("Looking in: %s" % j.outputdir)
            print("ls: %s" % str(os.listdir(j.outputdir)))
            assert os.path.isfile(os.path.join(j.outputdir, self.outputfile)), 'File must exist'

            assert os.path.isdir(os.path.join(j.outputdir, self.subdir_path)), 'Directory must exist'
            assert os.path.isfile(os.path.join(j.outputdir, self.subdir_path, self.outputfile)), 'File in directory must exist'

    def testStructureOfMerger(self):
        """Test that structure in the output sandbox is recreated in the merge"""
        from Ganga.GPI import TextMerger
        self.runJobSlice()

        tm = TextMerger()
        tm.files = [self.outputfile, os.path.join(self.subdir_path, self.outputfile)]

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        assert tm.merge(self.jobslice, tmpdir), 'Merge must run correctly'
        assert os.path.exists(os.path.join(tmpdir, self.outputfile)), 'Merge must produce the file'

    def testOutputEqualsInput(self):
        """Tests that setting outputdir == inputdir fails always"""
        from Ganga.GPI import TextMerger

        self.runJobSlice()
        tm = TextMerger(overwrite=True)
        tm.files = [self.outputfile]

        for j in self.jobslice:
            with pytest.raises(PostProcessException):
                tm.merge(self.jobslice, outputdir=j.outputdir)

