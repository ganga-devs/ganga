################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: TestStructure.py,v 1.1 2008-07-17 16:41:12 moscicki Exp $
################################################################################
from __future__ import division
from GangaTest.Framework.tests import GangaGPITestCase
from GangaTest.Framework.utils import sleep_until_completed,write_file
from Ganga.GPIDev.Adapters.IPostProcessor import PostProcessException
import os
import tempfile

class TestStructure(GangaGPITestCase):

    def __init__(self):

        self.jobslice = []
        self.file_name = 'id_echo.sh'

    def setUp(self):

        for _ in range(2):

            j = Job(application=Executable(),backend=Local())

            scriptString = '''
            #!/bin/sh
            echo "Output from job $1." > out.txt
            mkdir -p subdir
            echo "Output from job $2." > subdir/out.txt
            '''

            #write string to tmpfile
            tmpdir = tempfile.mktemp()
            os.mkdir(tmpdir)
            fileName = os.path.join(tmpdir,self.file_name)

            write_file(fileName, scriptString)

            j.application.exe = 'sh'
            j.application.args = [File(fileName), str(j.id),str(j.id*10)]
            j.outputfiles = [OutputSandboxFile('out.txt'),OutputSandboxFile('subdir/out.txt')]
            self.jobslice.append(j)

    def runJobSlice(self):

        for j in self.jobslice:
            j.submit()
            
            if not sleep_until_completed(j):
                assert False, 'Test timed out' 
            assert j.status == 'completed'

    def testStructureCreated(self):

        self.runJobSlice()

        for j in self.jobslice:

            assert os.path.exists(os.path.join(j.outputdir,'out.txt')), 'File must exist'
            assert os.path.exists(os.path.join(j.outputdir,'subdir','out.txt')), 'File in directory must exist'

    def testStructureOfMerger(self):
        """Test that structure in the output sandbox is recreated in the merge"""
        self.runJobSlice()

        tm = TextMerger()
        tm.files = ['out.txt','subdir/out.txt']

        tmpdir = tempfile.mktemp()
        os.mkdir(tmpdir)

        assert tm.merge(self.jobslice,tmpdir), 'Merge must run correctly'
        assert os.path.exists(os.path.join(tmpdir,'out.txt')), 'Merge must produce the file'
        
        

    def testOutputEqualsInput(self):
        """Tests that setting outputdir == inputdir fails always"""
        
        self.runJobSlice()
        tm = TextMerger(overwrite = True)
        tm.files = ['out.txt']

        for j in self.jobslice:
            try: tm.merge(self.jobslice,outputdir = j.outputdir)
            except PostProcessException: pass
            else: assert False, 'Must raise PostProcessException'


        
        
