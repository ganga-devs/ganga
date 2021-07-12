from GangaCore.testlib.monitoring import run_until_completed
from GangaCore.testlib.monitoring import run_until_state


def test_job_a_create(gpi):
    j = gpi.Job(backend=gpi.Condor())

def test_job_b_completed(gpi):
    j = gpi.Job(backend=gpi.Condor())
    j.submit()
    assert run_until_completed(j)

def test_job_c_failed(gpi):
    j = gpi.Job(backend=gpi.Condor())
    j.application.exe = 'exit'
    j.application.args = [1]
    j.submit()
    assert run_until_state(j, 'failed', 300, ['new', 'killed', 'unknown', 'removed', 'completed'])

def test_job_d_kill(gpi):

    j = gpi.Job(backend=gpi.Condor())
    j.application.exe = 'sleep'
    j.application.args = [120]

    j.submit()
    assert run_until_state(j, 'running', 300, ['new', 'killed', 'failed', 'unknown', 'removed', 'completed'])

    j.kill()
    assert run_until_state(j, 'killed', 300, ['new', 'failed', 'unknown', 'removed', 'completed'])

