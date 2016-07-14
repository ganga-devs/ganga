from Ganga.GPIDev.Base.Proxy import stripProxy

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


def test_submit_monitor(gpi):
    from Ganga.GPI import Job, LCG
    j = Job()
    j.backend = LCG()

    job_id = 'https://example.com:9000/42'

    with patch('Ganga.Lib.LCG.Grid.submit', return_value=job_id) as submit:
        j.submit()
        submit.assert_called_once()
        assert j.backend.id == job_id

    status_info = {
        'status': 'Submitted',
        'name': '',
        'destination': '',
        'reason': '',
        'exit': '',
        'is_node': False,
        'id': job_id
    }

    status_results = [
        ([status_info], []),  # Once for the proper status call
        ([], [])  # Once for the bulk monitoring call
    ]

    with patch('Ganga.Lib.LCG.Grid.status', side_effect=status_results) as status:
        stripProxy(j).backend.master_updateMonitoringInformation([stripProxy(j)])
        assert status.call_count == 2
