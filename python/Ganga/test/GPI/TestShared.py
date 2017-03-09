from __future__ import absolute_import

from os import path

from Ganga.testlib.GangaUnitTest import GangaUnitTest


class TestShared(GangaUnitTest):

    shared_area_location = None

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts = [('TestingFramework', 'AutoCleanup', 'False')]
        super(TestShared, self).setUp(extra_opts=extra_opts)

    def test_A_Construction(self):
        from Ganga.GPI import Job
        j = Job()

        assert(j.application.is_prepared == None)
        
        j.prepare()

        assert(j.application.is_prepared != None)
        
        self.shared_area_location = j.application.is_prepared.path()
        assert(path.isdir(self.shared_area_location))
        

    def test_B_Persistency(self):
        """
        Test that the shared area is the same after Ganga restarts
        """
        from Ganga.GPI import jobs

        j = jobs[-1]

        assert(j.application.is_prepared != None)

        assert(self.shared_area_location == j.application.is_prepared.path())

        assert(path.isdir(j.application.is_prepared.path()))

        
    def test_C_Unprepare(self):
        """Make sure that unprepare restore None for the shared area."""
        from Ganga.GPI import jobs
        j = jobs[-1]

        assert(j.application.is_prepared != None)

        j.unprepare()

        assert(j.application.is_prepared == None)


    def test_D_Cleanup(self):
        """Test that shared area is removed when counter goes to zero"""
        from Ganga.GPI import jobs
        j = jobs[-1]

        j.prepare()

        sharedir = j.application.is_prepared.path()
        assert(path.isdir(sharedir))
        
        j.unprepare()

        assert(not path.isdir(sharedir))
        
