from __future__ import absolute_import

from Ganga.testlib.mark import external


def test_job_create(gpi):
    from Ganga.GPI import Job, Dirac

    j = Job(backend=Dirac())


@external
def test_job_submit(gpi):
    from Ganga.GPI import Job, Dirac

    j = Job(backend=Dirac())
    j.submit()
