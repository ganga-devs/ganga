from __future__ import absolute_import

from os import makedirs, path
import shutil
from tempfile import gettempdir

from Ganga.GPIDev.Base.Proxy import stripProxy
from Ganga.testlib.GangaUnitTest import GangaUnitTest

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

        assert path.exists(gaudi_testOpts)

        gr = GaudiRun(directory=gaudi_testFol, myOpts=LocalFile(gaudi_testOpts))

        assert isinstance(stripProxy(gr).getOptsFile(), stripProxy(LocalFile))
        assert stripProxy(gr).getDir()

        reconstructed_path = path.join(stripProxy(gr).getOptsFile().localDir, stripProxy(gr).getOptsFile().namePattern)

        assert reconstructed_path == gaudi_testOpts

        assert open(reconstructed_path).read() == "print('hello')"

        assert stripProxy(gr).getDir() == gaudi_testFol

        j=Job()
        j.application = gr

        assert isinstance(j.application, GaudiRun)

        df = DiracFile(lfn='/not/some/file')

        gr.myOpts = df

        assert gr.myOpts.lfn == df.lfn

        shutil.rmtree(gaudi_testFol, ignore_errors=True)

