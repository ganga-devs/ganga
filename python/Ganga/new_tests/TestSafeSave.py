import unittest

class TestSafeSave(unittest.TestCase):

    def test_safe_save_threadcalls(self):
        "Test that XML files don't disappear - See Github Issue #185"
        import threading
        import os

        from Ganga.Core.GangaRepository.GangaRepositoryXML import safe_save

        def my_to_file(obj, fhandle, ignore_subs):
            fhandle.write("!" * 1000)

        # Create lots of threads that will keep hitting safe_save
        testfn = '/tmp/xmltest.tmp'
        ths = []

        if os.path.isfile(testfn):
            os.remove(testfn)

        for i in range(0, 500):
            ths.append( threading.Thread(target=safe_save, args=(testfn, [], my_to_file ) ) )

        for th in ths:
            th.start()

        for th in ths:
            th.join()

        self.assertTrue(os.path.isfile(testfn))
