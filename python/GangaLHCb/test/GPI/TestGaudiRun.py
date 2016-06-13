from __future__ import absolute_import

from Ganga.testlib.GangaUnitTest import GangaUnitTest
from Ganga.GPIDev.Base.Proxy import stripProxy
from os import makedirs, path
from tempfile import gettempdir
import shutil

class TestGaudiRun(GangaUnitTest):

    def testInternal(self):

        from Ganga.GPI import GaudiRun, Job, LocalFile, DiracFile

        tmp_fol = gettempdir()
        gaudi_testFol = path.join(tmp_fol, 'GaudiRunTest')
        shutil.rmtree(gaudi_testFol, ignore_errors=True)
        makedirs(gaudi_testFol)
        gaudi_testOpts = path.join(gaudi_testFol, 'testOpts.py')
        with open(gaudi_testOpts, 'w+') as temp_opt:
            temp_opt.write("print('hello')")

        gr = GaudiRun(directory=gaudi_testFol, myOpts=LocalFile(gaudi_testOpts))

        assert isinstance(stripProxy(gr).getOptsFile(), stripProxy(LocalFile))
        assert stripProxy(gr).getDir()

        assert open(path.join(stripProxy(gr).getOptsFile().localDir, stripProxy(gr).getOptsFile().namePattern)).read() == "print('hello')"

        assert stripProxy(gr).getDir() == gaudi_testFol

        j=Job()
        j.application = gr

        assert isinstance(j.application, GaudiRun)

        df = DiracFile(lfn='/not/some/file')

        gr.myOpts = df

        assert gr.myOpts.lfn == df.lfn

        shutil.rmtree(gaudi_testFol, ignore_errors=True)

