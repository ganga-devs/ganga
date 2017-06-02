from Ganga.GPIDev.Base.Proxy import stripProxy

try:
    from unittest.mock import patch
except ImportError:
    from mock import patch

from Ganga.testlib.mark import external, requires_cred
from Ganga.testlib.monitoring import run_until_completed

from Ganga.GPIDev.Credentials.VomsProxy import VomsProxy

@external
@requires_cred(VomsProxy(), 'LCG Requires a Voms proxy for testing')
def test_job_submit_and_monitor(gpi):
    from Ganga.GPI import Job, LCG

    j = Job()
    j.backend = LCG()
    j.submit()

    assert j.status != 'new'
    stripProxy(LCG).master_updateMonitoringInformation([stripProxy(j)])

@external
@requires_cred(VomsProxy(), 'LCG Requires a Voms proxy for testing')
def test_job_kill(gpi):
    from Ganga.GPI import Job, LCG

    j = Job()
    j.backend = LCG()
    j.submit()
    j.kill()

@requires_cred(VomsProxy(), 'LCG Requires a Voms proxy for testing')
def test_submit_kill_resubmit(gpi):
    """
    Test that a simple submit-kill-resubmit-kill cycle works
    """

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

    with patch('Ganga.Lib.LCG.Grid.cancel', return_value=True):
        j.kill()

@requires_cred(VomsProxy(), 'LCG Requires a Voms proxy for testing')
def test_submit_monitor(gpi):
    """
    Test that an LCG job can be monitored
    """

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
        assert status.call_count >= 1

    with patch('Ganga.Lib.LCG.Grid.cancel', return_value=True):
        j.kill()

