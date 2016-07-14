try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from Ganga.testlib.mark import external
from Ganga.testlib.monitoring import run_until_completed


@external
def test_job_complete(gpi):
    from Ganga.GPI import Job, LCG

    j = Job()
    j.backend = LCG()
    j.submit()
    assert run_until_completed(j, timeout=1200, sleep_period=10), 'Timeout on job submission: job is still not finished'


@external
def test_job_kill(gpi):
    from Ganga.GPI import Job, LCG

    j = Job()
    j.backend = LCG()
    j.submit()
    j.kill()


def test_submit_kill_resubmit(gpi):
    from Ganga.GPI import Job, LCG
    j = Job()
    j.backend = LCG()

    with patch('Ganga.Lib.LCG.Grid.submit', return_value='https://example.com:9000/42') as submit:
        j.submit()
        submit.assert_called_once()
        assert j.backend.id == 'https://example.com:9000/42'

    with patch('Ganga.Lib.LCG.Grid.cancel', return_value=True) as cancel:
        j.kill()
        cancel.assert_called_once()
        assert j.status == 'killed'

    with patch('Ganga.Lib.LCG.Grid.submit', return_value='https://example.com:9000/43') as submit:
        j.resubmit()
        submit.assert_called_once()
        assert j.backend.id == 'https://example.com:9000/43'
