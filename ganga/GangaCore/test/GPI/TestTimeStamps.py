from GangaTest.Framework.utils import sleep_until_completed, sleep_until_state
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

    j = gpi.Job(backend=gpi.Local(batchsize=3))

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

    j = gpi.Job(backend=gpi.Local(batchsize=3))
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

    j3 = gpi.Job(backend=gpi.Local(batchsize=3))

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

# Test for timestamps of states in Local backend
def test_statetime_local(gpi):

    # Job 1 of 6 -------

    gpi.config['Configuration']['resubmitOnlyFailedSubjobs']=False

    j = gpi.Job()

    j.submit()

    assert sleep_until_state(j, 30, 'completed')

    assert j.time.submitted() == j.time.timestamps['submitted']
    assert isinstance(j.time.submitted(), datetime.datetime)

    # Job 2 of 6 -------

    j_comp = gpi.Job()
    j_comp.application.exe='sleep'
    j_comp.application.args=[30]

    j_comp.submit()
    assert sleep_until_state(j_comp, 30, 'running')

    j_comp.kill()
    assert sleep_until_state(j_comp, 30, 'killed')

    j_comp.resubmit()
    assert sleep_until_state(j_comp, 45, 'completed')

    assert isinstance(j_comp.time.new(), datetime.datetime)
    assert isinstance(j_comp.time.submitting(), datetime.datetime)
    assert isinstance(j_comp.time.timestamps['resubmitted'], datetime.datetime)
    assert isinstance(j_comp.time.backend_running(), datetime.datetime)
    assert isinstance(j_comp.time.backend_final(), datetime.datetime)
    assert isinstance(j_comp.time.final(), datetime.datetime)

    # Job 3 of 6 -------
            
    j_fail = gpi.Job()
    j_fail.application.exe='sleep'
    j_fail.application.args='300'

    j_fail.submit()
    assert sleep_until_state(j_fail, 30, 'running')

    j_fail.force_status('failed')
    assert sleep_until_state(j_fail, 30, 'failed')

    assert isinstance(j_fail.time.new(), datetime.datetime)
    assert isinstance(j_fail.time.submitting(), datetime.datetime)
    assert isinstance(j_fail.time.submitted(), datetime.datetime)
    assert isinstance(j_fail.time.backend_running(), datetime.datetime)
    assert isinstance(j_fail.time.final(), datetime.datetime)

    # Job 4 of 6 (With subjobs) -------

    j = gpi.Job(backend=gpi.Local(batchsize=3))
    j.application.exe='sleep'
    j.splitter='ArgSplitter'
    j.splitter.args=[[10],[10],[10]]

    j.submit()
    assert sleep_until_state(j, 120, 'completed')

    assert j.time.submitted() == j.time.timestamps['submitted']
    assert isinstance(j.time.submitted(), datetime.datetime)

    # Job 5 of 6 (With subjobs) -------

    j_comp = gpi.Job(backend=gpi.Local(batchsize=3))

    j_comp.application.exe='sleep'
    j_comp.splitter='ArgSplitter'
    j_comp.splitter.args=[[30],[30],[30]]

    j_comp.submit()
    assert sleep_until_state(j_comp, 120, 'running')

    j_comp.subjobs.kill()
    assert sleep_until_state(j_comp, 120, 'killed')

    j_comp.resubmit()
    assert sleep_until_state(j_comp, 120, 'completed')

    assert isinstance(j_comp.time.new(), datetime.datetime)
    assert isinstance(j_comp.time.submitting(), datetime.datetime), "Job %d" %j_comp.id
    assert isinstance(j_comp.time.timestamps['resubmitted'], datetime.datetime)
    assert isinstance(j_comp.time.backend_running(), datetime.datetime)
    assert isinstance(j_comp.time.backend_final(), datetime.datetime)
    assert isinstance(j_comp.time.final(), datetime.datetime)

    # Job 6 of 6 (With subjobs) -------

    j_fail = gpi.Job(backend=gpi.Local(batchsize=3))
    j_fail.splitter='ArgSplitter'
    j_fail.splitter.args=[[],[],[]]
    j_fail.application.exe='sleep'
    j_fail.application.args='60'

    j_fail.submit()

    j_fail.force_status('failed')
    assert sleep_until_state(j_fail, 120, 'failed')

    assert isinstance(j_fail.time.new(), datetime.datetime)
    assert isinstance(j_fail.time.submitting(), datetime.datetime)
    assert isinstance(j_fail.time.submitted(), datetime.datetime)
    if 'backend_running' in j_fail.time.timestamps.keys():
        assert isinstance(j_fail.time.backend_running(), datetime.datetime)
    else:
        pass
    assert isinstance(j_fail.time.final(), datetime.datetime)

def test_timestamp_details_local(gpi):

    from GangaTest.Framework.utils import sleep_until_completed
    import datetime

    # Without Subjobs -------

    j = gpi.Job()
    j.submit()

    assert sleep_until_completed(j,180)
    assert isinstance(j.time.details(), dict)

    # With Subjobs -------

    j = gpi.Job(backend=gpi.Local(batchsize=3))
    j.splitter='ArgSplitter'
    j.splitter.args=[[],[],[]]
    j.submit()

    assert sleep_until_completed(j,180)
    assert not isinstance(j.time.details(), dict)
    for i in range(0,len(j.subjobs)):
            assert isinstance(j.time.details(i), dict)

def test_subjobs_stamporder_local(gpi):

    from GangaTest.Framework.utils import sleep_until_completed

    j = gpi.Job(backend=gpi.Local(batchsize=3))
    j.splitter='ArgSplitter'
    j.splitter.args=[[],[],[]]
    j.submit()

    assert sleep_until_completed(j,500)

    # timestamp: submitted 

    sj_stamplist = []
    for sjs in j.subjobs:
            sj_stamplist.append(sjs.time.timestamps['submitted'])

    sj_stamplist.sort()

    assert j.time.timestamps['submitted'] == sj_stamplist[0]

    # timestamp: backend_running

    sj_stamplist = []
    for sjs in j.subjobs:
            sj_stamplist.append(sjs.time.timestamps['backend_running'])

    sj_stamplist.sort()

    assert j.time.timestamps['backend_running'] == sj_stamplist[0]

    # timestamp: backend_final

    sj_stamplist = []
    for sjs in j.subjobs:
            sj_stamplist.append(sjs.time.timestamps['backend_final'])

    sj_stamplist.sort()

    assert j.time.timestamps['backend_final'] == sj_stamplist[len(sj_stamplist)-1]

    # timestamp: final

    sj_stamplist = []
    for sjs in j.subjobs:
            sj_stamplist.append(sjs.time.timestamps['final'])

    sj_stamplist.sort()

    assert j.time.timestamps['final'] == sj_stamplist[len(sj_stamplist)-1]

def test_new_subjob_not_overwrite_local(gpi):

    from GangaTest.Framework.utils import sleep_until_completed
    import datetime

    j = gpi.Job(backend=gpi.Local(batchsize=3))

    t_new_1 = j.time.new()
    t1 = datetime.datetime.now()
    t2 = t1

    while (t2-t1)<datetime.timedelta(0, 10, 0):
            t2 = datetime.datetime.now()

    j.splitter='ArgSplitter'
    j.splitter.args=[[],[],[]]
    j.submit()

    assert sleep_until_completed(j, 180)

    for sjs in j.subjobs:
            assert sjs.time.new() > t_new_1

    assert j.time.new() == t_new_1, "old 'new':%s, new 'new': %s" %(str(t_new_1), str(j.time.new()))
    
