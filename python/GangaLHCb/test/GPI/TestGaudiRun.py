from __future__ import absolute_import

import pytest
from Ganga.testlib.mark import external
from Ganga.testlib.GangaUnitTest import GangaUnitTest
from os import makedirs, path
from tempfile import gettempdir

class TestGaudiRun(GangaUnitTest):

    def testInternal(self):

        from Ganga.GPI import GaudiRun, Job, LocalFile, DiracFile

        tmp_fol = gettempdir()
        gaudi_testFol = path.join(tmp_fol, 'GaudiRunTest')
        makedirs(gaudi_testFol)
        gaudi_testOpts = path.join(gaudi_testFol, 'testOpts.py')
        with open(gaudi_testOpts, 'w+') as temp_opt:
            temp_opt.write("print('hello')")

        gr = GaudiRun(directory=gaudi_testFol, myOpts=LocalFile(gaudi_testOpts))

        assert isinstance(gr.getOptsFile(), LocalFile)
        assert gr.getDir()

        assert open(path.join(gr.getOptsFile().namePattern, gr.getOptsFile().localDir)).read() == "print('hello')"

        assert gr.getDir() == gaudi_testFol

        j=Job()
        j.application = gr

        assert isinstance(j.application, GaudiRun)

        df = DiracFile(lfn='/not/some/file')

        gr.myOpts = df

        assert gr.myOpts == df

