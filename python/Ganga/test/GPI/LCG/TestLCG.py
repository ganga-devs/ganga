from Ganga.testlib.mark import external
from Ganga.testlib.monitoring import run_until_completed


#@external
def test_job_complete(gpi):
    from Ganga.GPI import Job, LCG

    j = Job()
    j.backend = LCG()
    j.submit()
    assert run_until_completed(j, timeout=600, sleep_period=10), 'Timeout on job submission: job is still not finished'


#@external
def test_job_kill(gpi):
    from Ganga.GPI import Job, LCG

    j = Job()
    j.backend = LCG()
    j.submit()
    j.kill()
