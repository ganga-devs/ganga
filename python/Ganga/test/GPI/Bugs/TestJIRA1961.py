

from Ganga.testlib.decorators import add_config


@add_config([('Preparable', 'unprepare_on_copy', 'True')])
def test_unprepareTrue(gpi):
    from Ganga.GPI import Job, Executable
    j = Job(application=Executable(exe='/bin/echo', args=['hello']))
    j.submit()

    assert j.application.is_prepared is not None

    j2 = j.copy()

    assert j2.application.is_prepared is None

    j3 = Job(j)

    assert j3.application.is_prepared is None


@add_config([('Preparable', 'unprepare_on_copy', 'False')])
def test_unprepareFalse(gpi):

    from Ganga.GPI import Job, Executable
    k = Job(application=Executable(exe='/bin/echo', args=['hello']))
    k.submit()

    assert k.application.is_prepared is not None

    k2 = k.copy()

    assert k2.application.is_prepared is not None

    k3 = Job(k)

    assert k.application.is_prepared is not None
