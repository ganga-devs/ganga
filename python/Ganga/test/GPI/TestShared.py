from __future__ import absolute_import

from os import path

from Ganga.testlib.GangaUnitTest import GangaUnitTest

from Ganga.Core.GangaRepository import getRegistry


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
        
        TestShared.shared_area_location = j.application.is_prepared.path()
        assert(path.isdir(TestShared.shared_area_location))
        

    def test_B_Persistency(self):
        """
        Test that the shared area is the same after Ganga restarts
        """
        from Ganga.GPI import jobs

        j = jobs[-1]

        assert(j.application.is_prepared != None)

        assert(TestShared.shared_area_location == j.application.is_prepared.path())

        assert(path.isdir(j.application.is_prepared.path()))

        
    def test_C_Unprepare(self):
        """Make sure that unprepare restore None for the shared area."""
        from Ganga.GPI import jobs
        j = jobs[-1]

        assert(j.application.is_prepared != None)

        TestShared.shared_area_location == j.application.is_prepared.path()

        j.unprepare()

        assert(j.application.is_prepared == None)


    def test_D_Cleanup(self):
        """Test that shared area is removed when counter goes to zero"""
        from Ganga.GPI import jobs
        j = jobs[-1]

        assert(not path.isdir(TestShared.shared_area_location))

        j.prepare()

        sharedir = j.application.is_prepared.path()
        assert(path.isdir(sharedir))

    def test_E_increment(self):
        """Test that the ref counter is incremented as appropriate"""

        from Ganga.GPI import jobs

        j=jobs[-1]

        shareRef = getRegistry('prep').getShareRef()

        refDict = shareRef.name

        assert j.application.is_prepared.name in refDict

        j2 = j.copy()

        assert refDict[j.application.is_prepared.name] is 2

    def test_F_decrement(self):
        """Test that the ref counter is decremented as appropriate"""

        from Ganga.GPI import jobs

        j=jobs[-1]

        shareRef = getRegistry('prep').getShareRef()

        refDict = shareRef.name

        assert refDict[j.application.is_prepared.name] is 2

        this_ref = j.application.is_prepared.name
        this_path = j.application.is_prepared.path()

        j.unprepare()

        assert refDict[this_ref] is 1

        j2=jobs[-2].copy()

        assert refDict[this_ref] is 2

        j2.remove()

        assert refDict[this_ref] is 1

        jobs[-2].remove()

        assert refDict[this_ref] is 0

        assert not path.isdir(this_path)

