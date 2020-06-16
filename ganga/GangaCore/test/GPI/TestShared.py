
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
from os import path

from GangaCore.testlib.GangaUnitTest import GangaUnitTest

from GangaCore.Core.GangaRepository import getRegistry



class TestShared(GangaUnitTest):

    shared_area_location = None

    def setUp(self):
        """Make sure that the Job object isn't destroyed between tests"""
        extra_opts = [('TestingFramework', 'AutoCleanup', 'False')]
        super(TestShared, self).setUp(extra_opts=extra_opts)

    def test_A_Construction(self):
        from GangaCore.GPI import Job, LocalFile
        j = Job()

        assert(j.application.is_prepared is None)
        
        j.prepare()

        assert(j.application.is_prepared is not None)
        
        TestShared.shared_area_location = j.application.is_prepared.path()
        assert(path.isdir(TestShared.shared_area_location))

        TestShared.a_file_location = path.join(j.application.is_prepared.path(), 'a.txt')
        TestShared.b_file_location = path.join(j.application.is_prepared.path(), 'b.txt')

        open(TestShared.a_file_location, 'w').close()
        open(TestShared.b_file_location, 'w').close()
        j.application.is_prepared.associated_files.append(LocalFile(TestShared.a_file_location))
        j.application.is_prepared.associated_files.append(LocalFile(TestShared.b_file_location))


    def test_B_Persistency(self):
        """
        Test that the shared area is the same after Ganga restarts
        """
        from GangaCore.GPI import jobs

        j = jobs[-1]

        assert(j.application.is_prepared is not None)

        assert(TestShared.shared_area_location == j.application.is_prepared.path())

        assert(path.isdir(j.application.is_prepared.path()))

        for lf in j.application.is_prepared.associated_files:
            assert (path.join(lf.localDir, lf.namePattern) in [TestShared.a_file_location, TestShared.b_file_location])
            assert (path.isfile(path.join(lf.localDir, lf.namePattern)))

        
    def test_C_Unprepare(self):
        """Make sure that unprepare restore None for the shared area."""
        with patch('builtins.input', return_value='y') as _raw_input:
            from GangaCore.GPI import jobs
            j = jobs[-1]

            assert(j.application.is_prepared is not None)

            TestShared.shared_area_location == j.application.is_prepared.path()

            j.unprepare()

            assert(j.application.is_prepared is None)

            assert(not path.isfile(TestShared.a_file_location))
            assert(not path.isfile(TestShared.b_file_location))


    def test_D_Cleanup(self):
        """Test that shared area is removed when counter goes to zero"""
        from GangaCore.GPI import jobs
        j = jobs[-1]

        assert(not path.isdir(TestShared.shared_area_location))
        assert(not path.isfile(TestShared.a_file_location))
        assert(not path.isfile(TestShared.b_file_location))

        j.prepare()

        sharedir = j.application.is_prepared.path()
        assert(path.isdir(sharedir))

    def test_E_increment(self):
        """Test that the ref counter is incremented as appropriate"""

        from GangaCore.GPI import jobs

        j=jobs[-1]

        shareRef = getRegistry('prep').getShareRef()

        refDict = shareRef.name

        assert j.application.is_prepared.name in refDict

        j2 = j.copy()

        assert refDict[j.application.is_prepared.name] is 2

    def test_F_decrement(self):
        """Test that the ref counter is decremented as appropriate"""

        from GangaCore.GPI import jobs

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

