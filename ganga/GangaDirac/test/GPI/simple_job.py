from __future__ import absolute_import

from GangaCore.testlib.mark import external


def test_job_create(gpi):
    from GangaCore.GPI import Job, Dirac

    j = Job(backend=Dirac())


@external
def test_job_submit(gpi):
    from GangaCore.GPI import Job, Dirac

    j = Job(backend=Dirac())
    j.submit()
