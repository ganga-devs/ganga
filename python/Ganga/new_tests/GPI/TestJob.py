from GangaUnitTest import GangaUnitTest

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
        self.assertTrue( isinstance(j, Job) )
        self.assertEqual( j.application.exe, "sleep" )
        self.assertEqual( j.application.args, ["myarg"] )
        self.assertTrue( isinstance(j.backend, ARC) )
        self.assertEqual( j.backend.CE, "my.ce" )
        self.assertTrue( isinstance(j.inputdata, GangaDataset) )
        self.assertEqual( len(j.inputdata.files), 1 )
        self.assertTrue( isinstance(j.inputdata.files[0], LocalFile) )
        self.assertEqual( j.inputdata.files[0].namePattern, "*.txt" )
        self.assertEqual( len(j.inputfiles), 1 )
        self.assertTrue( isinstance(j.inputfiles[0], LocalFile) )
        self.assertEqual( j.inputfiles[0].namePattern, "*.txt" )
        self.assertEqual( j.name, "testname" )
        self.assertEqual( len(j.outputfiles), 1 )
        self.assertTrue( isinstance(j.outputfiles[0], LocalFile) )
        self.assertEqual( j.outputfiles[0].namePattern, "*.txt" )
        self.assertEqual( len(j.postprocessors), 1 )
        self.assertTrue( isinstance(j.postprocessors[0], FileChecker) )
        self.assertEqual( j.postprocessors[0].files, ["stdout"] )
        self.assertEqual( j.postprocessors[0].searchStrings, ["my search"] )
        self.assertTrue( isinstance(j.splitter, GenericSplitter) )
        self.assertEqual( j.splitter.attribute, "application.args" )
        self.assertEqual( j.splitter.values, ['arg 1', 'arg 2', 'arg 3'])

    def testJobCopy(self):
        """Test that a job copy copies everything properly"""
        from Ganga.GPI import Job, ARC, GenericSplitter, GangaDataset, LocalFile, FileChecker


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
        self.assertTrue( isinstance(j2, Job) )
        self.assertEqual( j2.application.exe, "sleep" )
        self.assertEqual( j2.application.args, ["myarg"] )
        self.assertTrue( isinstance(j2.backend, ARC) )
        self.assertEqual( j2.backend.CE, "my.ce" )
        self.assertTrue( isinstance(j2.inputdata, GangaDataset) )
        self.assertEqual( len(j2.inputdata.files), 1 )
        self.assertTrue( isinstance(j2.inputdata.files[0], LocalFile) )
        self.assertEqual( j2.inputdata.files[0].namePattern, "*.txt" )
        self.assertEqual( len(j2.inputfiles), 1 )
        self.assertTrue( isinstance(j2.inputfiles[0], LocalFile) )
        self.assertEqual( j2.inputfiles[0].namePattern, "*.txt" )
        self.assertEqual( j2.name, "testname" )
        self.assertEqual( len(j2.outputfiles), 1 )
        self.assertTrue( isinstance(j2.outputfiles[0], LocalFile) )
        self.assertEqual( j2.outputfiles[0].namePattern, "*.txt" )
        self.assertEqual( len(j2.postprocessors), 1 )
        self.assertTrue( isinstance(j2.postprocessors[0], FileChecker) )
        self.assertEqual( j2.postprocessors[0].files, ["stdout"] )
        self.assertEqual( j2.postprocessors[0].searchStrings, ["my search"] )
        self.assertTrue( isinstance(j2.splitter, GenericSplitter) )
        self.assertEqual( j2.splitter.attribute, "application.args" )
        self.assertEqual( j2.splitter.values, ['arg 1', 'arg 2', 'arg 3'])
