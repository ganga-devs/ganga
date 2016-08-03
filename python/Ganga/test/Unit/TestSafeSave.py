import uuid
import threading
import os

from Ganga.GPIDev.Lib.File.LocalFile import LocalFile


def test_safe_save_threadcalls():
    """Test that XML files don't disappear - See Github Issue #185"""

    from Ganga.Core.GangaRepository.GangaRepositoryXML import safe_save

    def my_to_file(obj, fhandle, ignore_subs):
        fhandle.write("!" * 1000)

    # Create lots of threads that will keep hitting safe_save
    testfn = '/tmp/xmltest.tmp' + str(uuid.uuid4())
    ths = []

    o = LocalFile()

    for i in range(0, 500):
        ths.append(threading.Thread(target=safe_save, args=(testfn, o, my_to_file)))

    for th in ths:
        th.start()

    for th in ths:
        th.join()

    assert os.path.isfile(testfn)
    os.remove(testfn)
    assert os.path.isfile(testfn + '~')
    os.remove(testfn + '~')
    assert not os.path.isfile(testfn + '.new')
