from __future__ import absolute_import, print_function

def test_jobTemplate(gpi):

    name_str = "TeStNaMe"

    jt = gpi.JobTemplate()
    jt.name = name_str

    j = gpi.Job(jt)

    assert jt.name == name_str
    assert j.name == jt.name

