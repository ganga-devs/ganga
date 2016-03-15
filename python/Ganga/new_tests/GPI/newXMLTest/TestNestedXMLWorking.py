from __future__ import absolute_import

from ..GangaUnitTest import GangaUnitTest

from .utilFunctions import getXMLFile

from os import path, unlink

def getNestedList():
    from Ganga.GPI import LocalFile, GangaList
    gl = GangaList()
    gl2 = GangaList()
    for i in range(5):
        gl.append(LocalFile())
    for i in range(5):
        gl2.append(gl)
    return gl2

class TestNestedXMLWorking(GangaUnitTest):

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        super(TestNestedXMLWorking, self).setUp()
        from Ganga.Utility.Config import setConfigOption
        setConfigOption('TestingFramework', 'AutoCleanup', 'False')

    def test_a_JobConstruction(self):
        """ First construct the Job object (singular)"""
        from Ganga.Utility.Config import getConfig
        self.assertFalse(getConfig('TestingFramework')['AutoCleanup'])

        from Ganga.GPI import Job, jobs, ArgSplitter
        j=Job()
        assert len(jobs) == 1

        j.splitter = ArgSplitter()
        j.splitter.args = getNestedList()

        assert j.splitter.args == getNestedList()

    def test_b_JobNotLoaded(self):
        """ Second get the job and check that getting it via jobs doesn't cause it to be loaded"""
        from Ganga.GPI import jobs

        assert len(jobs) == 1

        print("len: %s" % len(jobs))

        j = jobs(0)

        from Ganga.GPIDev.Base.Proxy import stripProxy
        raw_j = stripProxy(j)

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        assert not has_loaded_job

    def test_c_JobLoaded(self):
        """ Third do something to trigger a loading of a Job and then test if it's loaded"""
        from Ganga.GPI import jobs, ArgSplitter

        assert len(jobs) == 1

        j = jobs(0)

        from Ganga.GPIDev.Base.Proxy import stripProxy
        raw_j = stripProxy(j)

        ## ANY COMMAND TO LOAD A JOB CAN BE USED HERE
        raw_j.printSummaryTree()

        has_loaded_job = raw_j._getRegistry().has_loaded(raw_j)

        assert has_loaded_job

        assert isinstance(j.splitter, ArgSplitter)

        assert j.splitter.args == getNestedList()

    def test_d_testXMLContent(self):
        # Check content of XML is as expected
        from Ganga.Core.GangaRepository.VStreamer import to_file, from_file

        from Ganga.GPI import jobs, Job, ArgSplitter
        from Ganga.GPIDev.Base.Proxy import stripProxy

        from tempfile import NamedTemporaryFile

        j = jobs(0)
        assert path.isfile(getXMLFile(j))
        with open(getXMLFile(j)) as handler:
            tmpobj, errs = from_file(handler)

            assert tmpobj.splitter
            assert tmpobj.splitter.args == getNestedList()

            ignore_subs = ''

            with NamedTemporaryFile(delete=False) as new_temp_file:

                to_file(stripProxy(j), new_temp_file, ignore_subs)
                new_temp_file.flush()

            with NamedTemporaryFile(delete=False) as new_temp_file2:
                j2 = Job()
                j2.splitter = ArgSplitter()
                j2.splitter.args = getNestedList()

                to_file(stripProxy(j2), new_temp_file2, ignore_subs)
                new_temp_file2.flush()

            assert open(handler.name).read() == open(new_temp_file.name).read()
            assert open(handler.name) != open(new_temp_file2.name).read()

            unlink(new_temp_file.name)
            unlink(new_temp_file2.name)

