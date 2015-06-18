##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestTextMergerGzip.py,v 1.1 2009-02-03 09:31:12 wreece Exp $
##########################################################################
from __future__ import division
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed, write_file
import gzip
import os
import tempfile


def file_contains_gzip(filename, string):
    return gzip.GzipFile(filename).read().find(string) != -1


class TestTextMergerGzip(GangaGPITestCase):

    def __init__(self):

        self.jobslice = []
        self.file_name = 'id_echo.sh'

    def setUp(self):

        for i in range(4):

            j = Job(application=Executable(), backend=Local())

            scriptString = '''
            #!/bin/sh
            echo "Output from job $1." > out.txt
            echo "Output from job $2." > out2.txt
            gzip out.txt
            gzip out2.txt
            '''

            # write string to tmpfile
            tmpdir = tempfile.mktemp()
            os.mkdir(tmpdir)
            fileName = os.path.join(tmpdir, self.file_name)

            write_file(fileName, scriptString)

            j.application.exe = 'sh'
            j.application.args = [File(fileName), str(j.id), str(j.id * 10)]
            j.outputfiles = [LocalFile('out.txt.gz'), LocalFile('out2.txt.gz')]
            self.jobslice.append(j)

    def runJobSlice(self):

        for j in self.jobslice:
            j.submit()

            if not sleep_until_completed(j):
                assert False, 'Test timed out'
            assert j.status == 'completed'

    def tearDown(self):

        for j in self.jobslice:
            j.remove()

    def testDirectMerge(self):

        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        tm = TextMerger()
        tm.files = ['out.txt.gz', 'out2.txt.gz']
        assert tm.merge(self.jobslice, tmpdir), 'Merge should complete'

        output = os.path.join(tmpdir, 'out.txt.gz')
        assert os.path.exists(output), 'file must exist'
        for j in self.jobslice:
            assert file_contains_gzip(
                output, 'Output from job %d.' % j.id), 'File must contain the output of each individual job'

        output = os.path.join(tmpdir, 'out2.txt.gz')
        assert os.path.exists(output), 'file must exist'
        for j in self.jobslice:
            assert file_contains_gzip(output, 'Output from job %d.' % (
                j.id * 10)), 'File must contain the output of each individual job'

    def testDirectMergeGzip(self):

        self.runJobSlice()

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        tm = TextMerger()
        tm.files = ['out.txt.gz', 'out2.txt.gz']
        tm.compress = True
        assert tm.merge(self.jobslice, tmpdir), 'Merge should complete'

        output = os.path.join(tmpdir, 'out.txt.gz')
        assert os.path.exists(output), 'file must exist'
        for j in self.jobslice:
            assert file_contains_gzip(
                output, 'Output from job %d.' % j.id), 'File must contain the output of each individual job'

        output = os.path.join(tmpdir, 'out2.txt.gz')
        assert os.path.exists(output), 'file must exist'
        for j in self.jobslice:
            assert file_contains_gzip(output, 'Output from job %d.' % (
                j.id * 10)), 'File must contain the output of each individual job'
