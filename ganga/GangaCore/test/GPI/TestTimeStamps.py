from GangaTest.Framework.utils import sleep_until_completed
import datetime

# Test for checking timestamps for Local backend
def test_basic_timestamps_local(gpi):

    # Without Subjobs -------

    j = gpi.Job()

    assert 'new' in j.time.timestamps
    assert isinstance(j.time.new(), datetime.datetime)

    j.submit()
    sleep_until_completed(j,30)

    assert 'submitting' in j.time.timestamps
    assert isinstance(j.time.submitting(), datetime.datetime)

    assert 'submitted' in j.time.timestamps
    assert isinstance(j.time.submitted(), datetime.datetime)

    assert 'backend_running' in j.time.timestamps
    assert isinstance(j.time.backend_running(), datetime.datetime)

    assert 'backend_final' in j.time.timestamps
    assert isinstance(j.time.backend_final(), datetime.datetime)

    assert 'final' in j.time.timestamps
    assert isinstance(j.time.final(), datetime.datetime)

    # With Subjobs -------

    j = gpi.Job()

    j.splitter = 'ArgSplitter'
    j.splitter.args = [[],[],[]]

    j.submit()

    sleep_until_completed(j,30)

    assert 'submitting' in j.time.timestamps
    assert isinstance(j.time.submitting(), datetime.datetime)

    assert 'submitted' in j.time.timestamps
    assert isinstance(j.time.submitted(), datetime.datetime)

    assert 'backend_running' in j.time.timestamps
    assert isinstance(j.time.backend_running(), datetime.datetime)

    assert 'backend_final' in j.time.timestamps
    assert isinstance(j.time.backend_final(), datetime.datetime)

    assert 'final' in j.time.timestamps
    assert isinstance(j.time.final(), datetime.datetime)

    for sjs in j.subjobs:

        assert 'submitted' in sjs.time.timestamps
        assert isinstance(sjs.time.submitted(), datetime.datetime)

        assert 'backend_running' in sjs.time.timestamps
        assert isinstance(sjs.time.backend_running(), datetime.datetime)

        assert 'backend_final' in sjs.time.timestamps
        assert isinstance(sjs.time.backend_final(), datetime.datetime)

        assert 'final' in sjs.time.timestamps
        assert isinstance(sjs.time.final(), datetime.datetime)