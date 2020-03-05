
# Test kill and resubmit operations on a slice
def test_slices_operations(gpi):

    # Clear jobs before starting the test
    gpi.jobs.remove()

    j1 = gpi.Job()
    j1.application = gpi.Executable(exe='/usr/bin/env', args=['sleep','5'])
    j2 = j1.copy()
    j3 = j1.copy()

    j1.submit()
    j2.submit()
    j3.submit()

    # Kill j1 and j2
    gpi.jobs[:-1].kill()

    assert j1.status == "killed"
    assert j2.status == "killed"
    assert j3.status != "killed"

    # Resubmit j1 and j2
    gpi.jobs[:-1].resubmit()

    for j in gpi.jobs[:-1]:
        assert j.status != "killed"

    # Clear jobs registry
    gpi.jobs.remove()

    assert(len(gpi.jobs) == 0)

# Test slice copy
def test_slice_copy(gpi):
    
    # Clear registry before starting the test
    gpi.jobs.remove()

    # Create 4 new job
    gpi.Job()
    gpi.Job()
    gpi.Job()
    gpi.Job()

    # Duplicate above 4 jobs and put them in s
    s = gpi.jobs.copy()

    # length of s should be twice of length of jobs
    assert len(gpi.jobs) == 2 * len(s)

    # submit slice
    s.submit()

    # check if slice is submitted succesfully
    for j in s:
        assert j.status in ['submitted','running','completed']

# Test job ids in jobs registry
def test_ids_in_slice(gpi):

    # Clear registry before starting the test
    gpi.jobs.remove()

    # Create 4 new jobs and store the first job's id in a variable
    j1 = gpi.Job()
    first = j1.id
    gpi.Job()
    gpi.Job()
    gpi.Job()

    # Function to test if consecutive created jobs have consecutive IDs
    def test_ids(reg):
        assert reg.ids() == [first,first+1,first+2,first+3]

    test_ids(gpi.jobs)
    test_ids(gpi.jobs.select())
    test_ids(gpi.jobs[:])

# Test select attribute of slice
def test_slice_select_attributes(gpi):

    from GangaCore.Core.exceptions import GangaAttributeError

    j1 = gpi.Job()
    j1.application = gpi.Executable(exe='a')
    j1.backend = gpi.Local()

    j2 = gpi.Job()
    j2.application = gpi.Executable(exe='b')
    j2.backend = gpi.Interactive()

    j3 = gpi.Job()
    j3.application = gpi.Executable(exe='c')
    j3.backend = gpi.Local()

    # Attribute does not exist raise GangaAttributeError
    try:
        gpi.jobs.select(some_applications = gpi.Executable())
        assert False, "should raise GangaAttributeError"
    except GangaAttributeError:
        pass

    # All these are equivalent, only a class of the object is compared
    s = gpi.jobs.select(application = gpi.Executable())
    assert len(s) == len(gpi.jobs)

    s = gpi.jobs.select(application = gpi.Executable)
    assert len(s) == len(gpi.jobs)

    s = gpi.jobs.select(application = 'Executable')
    assert len(s) == len(gpi.jobs)

    # Test select attribute against backend of above created jobs
    s = gpi.jobs.select(backend = gpi.Local)
    assert j1 in s
    assert j3 in s
    assert j2 not in s

