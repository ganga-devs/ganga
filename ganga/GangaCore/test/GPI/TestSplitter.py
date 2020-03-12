def test_splitter_exception(gpi):

    from GangaTest.Framework.utils import assert_cannot_submit

    j = gpi.Job()
    j.splitter = gpi.TestSplitter()
    j.splitter.fail = "exception"

    assert_cannot_submit(j)


def test_subjob_app_config_fail(gpi):

    # This is a test in which the second subjob fails to submit because of the application config exception
    from GangaTest.Framework.utils import assert_cannot_submit

    j = gpi.Job()
    j.backend = gpi.TestSubmitter()
    j.application = gpi.TestApplication()
    j.splitter = gpi.GenericSplitter()
    j.splitter.attribute = 'application.fail'
    j.splitter.values = ['','config','']

    assert_cannot_submit(j)

    assert(len(j.subjobs)==0)
    
