from __future__ import absolute_import

from .GangaUnitTest import GangaUnitTest

from Ganga.GPIDev.Base.Proxy import stripProxy


class TestJob(GangaUnitTest):

    def testJobCreate(self):
        from Ganga.GPI import Job
        j = Job()

    def testJobSubmit(self):
        from Ganga.GPI import Job
        j = Job()
        j.submit()

    def testJobAssignment(self):
        """Test assignment of all job properties"""
        from Ganga.GPI import Job, ARC, GenericSplitter, GangaDataset, LocalFile, FileChecker
        from Ganga.GPIDev.Base.Proxy import isType

        j = Job()
        j.application.exe = "sleep"
        j.application.args = ['myarg']
        j.backend = ARC()
        j.backend.CE = "my.ce"
        j.inputdata = GangaDataset()
        j.inputdata.files = [ LocalFile("*.txt") ]
        j.inputfiles = [ LocalFile("*.txt") ]
        j.name = "testname"
        j.outputfiles = [ LocalFile("*.txt") ]
        j.postprocessors = FileChecker(files=['stdout'], searchStrings = ['my search'])
        j.splitter = GenericSplitter()
        j.splitter.attribute = "application.args"
        j.splitter.values = ['arg 1', 'arg 2', 'arg 3']

        # test all the assignments
        self.assertTrue( isType(j, Job) )
        self.assertEqual( j.application.exe, "sleep" )
        self.assertEqual( j.application.args, ["myarg"] )
        self.assertTrue( isType(j.backend, ARC) )
        self.assertEqual( j.backend.CE, "my.ce" )
        self.assertTrue( isType(j.inputdata, GangaDataset) )
        self.assertEqual( len(j.inputdata.files), 1 )
        self.assertTrue( isType(j.inputdata.files[0], LocalFile) )
        self.assertEqual( j.inputdata.files[0].namePattern, "*.txt" )
        self.assertEqual( len(j.inputfiles), 1 )
        self.assertTrue( isType(j.inputfiles[0], LocalFile) )
        self.assertEqual( j.inputfiles[0].namePattern, "*.txt" )
        self.assertEqual( j.name, "testname" )
        self.assertEqual( len(j.outputfiles), 1 )
        self.assertTrue( isType(j.outputfiles[0], LocalFile) )
        self.assertEqual( j.outputfiles[0].namePattern, "*.txt" )
        self.assertEqual( len(j.postprocessors), 1 )
        self.assertTrue( isType(j.postprocessors[0], FileChecker) )
        self.assertEqual( j.postprocessors[0].files, ["stdout"] )
        self.assertEqual( j.postprocessors[0].searchStrings, ["my search"] )
        self.assertTrue( isType(j.splitter, GenericSplitter) )
        self.assertEqual( j.splitter.attribute, "application.args" )
        self.assertEqual( j.splitter.values, ['arg 1', 'arg 2', 'arg 3'])

    def testJobCopy(self):
        """Test that a job copy copies everything properly"""
        from Ganga.GPI import Job, ARC, GenericSplitter, GangaDataset, LocalFile, FileChecker
        from Ganga.GPIDev.Base.Proxy import isType

        j = Job()
        j.application.exe = "sleep"
        j.application.args = ['myarg']
        j.backend = ARC()
        j.backend.CE = "my.ce"
        j.inputdata = GangaDataset()
        j.inputdata.files = [ LocalFile("*.txt") ]
        j.inputfiles = [ LocalFile("*.txt") ]
        j.name = "testname"
        j.outputfiles = [ LocalFile("*.txt") ]
        j.postprocessors = FileChecker(files=['stdout'], searchStrings = ['my search'])
        j.splitter = GenericSplitter()
        j.splitter.attribute = "application.args"
        j.splitter.values = ['arg 1', 'arg 2', 'arg 3']
        j2 = j.copy()

        # test the copy has worked
        self.assertTrue( isType(j2, Job) )
        self.assertEqual( j2.application.exe, "sleep" )
        self.assertEqual( j2.application.args, ["myarg"] )
        self.assertTrue( isType(j2.backend, ARC) )
        self.assertEqual( j2.backend.CE, "my.ce" )
        self.assertTrue( isType(j2.inputdata, GangaDataset) )
        self.assertEqual( len(j2.inputdata.files), 1 )
        self.assertTrue( isType(j2.inputdata.files[0], LocalFile) )
        self.assertEqual( j2.inputdata.files[0].namePattern, "*.txt" )
        self.assertEqual( len(j2.inputfiles), 1 )
        self.assertTrue( isType(j2.inputfiles[0], LocalFile) )
        self.assertEqual( j2.inputfiles[0].namePattern, "*.txt" )
        self.assertEqual( j2.name, "testname" )
        self.assertEqual( len(j2.outputfiles), 1 )
        self.assertTrue( isType(j2.outputfiles[0], LocalFile) )
        self.assertEqual( j2.outputfiles[0].namePattern, "*.txt" )
        self.assertEqual( len(j2.postprocessors), 1 )
        self.assertTrue( isType(j2.postprocessors[0], FileChecker) )
        self.assertEqual( j2.postprocessors[0].files, ["stdout"] )
        self.assertEqual( j2.postprocessors[0].searchStrings, ["my search"] )
        self.assertTrue( isType(j2.splitter, GenericSplitter) )
        self.assertEqual( j2.splitter.attribute, "application.args" )
        self.assertEqual( j2.splitter.values, ['arg 1', 'arg 2', 'arg 3'])

    def test_job_equality(self):
        """Check that copies of Jobs are equal to each other"""
        from Ganga.GPI import Job
        j = Job()
        j2 = j.copy()
        j3 = Job(j)
        assert j == j2
        assert j2 == j3
        assert stripProxy(j) == stripProxy(j2)
        assert stripProxy(j2) == stripProxy(j3)
