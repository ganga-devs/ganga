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


def test_duration_local(gpi):

    j = gpi.Job()
    j.splitter = 'ArgSplitter'
    j.splitter.args = [[],[],[]]

    assert j.time.submissiontime() == None
    assert j.time.runtime() == None
    assert j.time.waittime() == None

    j.submit()

    assert sleep_until_completed(j,300)

    assert isinstance(j.time.submissiontime(), datetime.timedelta)
    assert isinstance(j.time.waittime(), datetime.timedelta)
    assert isinstance(j.time.runtime(), datetime.timedelta)


    for sjs in j.subjobs:
        assert isinstance(sjs.time.submissiontime(), datetime.timedelta) # Submitting does not appear in subjobs currently.
        assert isinstance(sjs.time.waittime(), datetime.timedelta)
        assert isinstance(sjs.time.runtime(), datetime.timedelta)

# Test for checking timestamps for Job Copy in Local Backend
def test_basic_timestamps_copy_local(gpi):
    
    # Without Subjobs -------

    j1 = gpi.Job()

    assert 'new' in j1.time.timestamps.keys()

    assert isinstance(j1.time.new(), datetime.datetime)

    j2 = j1.copy()

    assert 'new' in j2.time.timestamps.keys()

    assert isinstance(j2.time.new(), datetime.datetime)

    assert j2.time.new() - j1.time.new() > datetime.timedelta(0,0,0), "j2 'new' is not more recent than j1 'new'"

    # With Subjobs -------

    j3 = gpi.Job()

    j3.splitter='ArgSplitter'
    j3.splitter.args=[[],[],[]]

    assert 'new' in j3.time.timestamps.keys()
    assert isinstance(j3.time.new(), datetime.datetime)

    j3.submit()

    assert sleep_until_completed(j3, 300)

    for sjs in j3.subjobs:
        assert 'new' in sjs.time.timestamps.keys()

    j4 = j3.copy()

    assert 'new' in j4.time.timestamps.keys()
    assert isinstance(j4.time.new(), datetime.datetime)

    assert j4.time.new() - j3.time.new() > datetime.timedelta(0,0,0), "j4 'new' is not more recent than j3 'new'"

    for sjs in j4.subjobs:
        assert 'new' not in sjs.time.timestamps.keys()

    j4.submit()

    assert sleep_until_completed(j4, 300)

    for i in range(0,len(j4.subjobs)):
        for j in range(0,len(j3.subjobs)):
            assert j4.subjobs(i).time.new() > j3.subjobs(j).time.new()



