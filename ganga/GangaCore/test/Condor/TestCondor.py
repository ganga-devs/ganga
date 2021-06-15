from GangaCore.testlib.monitoring import run_until_completed
from GangaCore.testlib.monitoring import run_until_state


def test_job_create(gpi):
    j = gpi.Job(backend=Condor())

def test_job_kill(gpi):

    j = gpi.Job(backend=Condor())
    j.application.exe = 'sleep'
    j.application.args = [120]

    j.submit()
    assert run_until_state(j, 'running', timeout=60)

    j.kill()
    assert run_until_state(j, 'killed', timeout=60)

def test_job_submit(gpi):
    j = gpi.Job(backend=Condor())
    j.submit()

def test_job_completed(gpi):
    j = gpi.Job(backend=Condor())
    j.submit()
    assert run_until_completed(j)

def test_job_failed(gpi):
    j = gpi.Job(backend=Condor())
    j.submit()
    j.application.exe = 'exit'
    j.application.args = [1]
    assert run_until_state(j, 'failed', timeout=60)
