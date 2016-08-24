from Ganga.testlib.decorators import add_config


@add_config([('TestingFramework', 'AutoCleanup', False)])
def test_all_exceptions(gpi):
    """Create all exceptions and make sure they behave correctly"""

    import Ganga.Core.exceptions
    test_str = "My Test Error"

    def exception_test(err_name):
        """Run tests on the given exception"""
        err_type = getattr(Ganga.Core.exceptions, err_name)
        err_obj = err_type(test_str)
        assert err_obj.args[0].find(test_str) != -1

    err_list = ["GangaException", "ApplicationConfigurationError", "ApplicationPrepareError",
                "IncompleteJobSubmissionError", "IncompleteKillError", "JobManagerError", "GangaAttributeError",
                "GangaValueError", "GangaIOError", "SplitterError", "ProtectedAttributeError", "ReadOnlyObjectError",
                "TypeMismatchError", "SchemaError", "SchemaVersionError"]

    for e in err_list:
        exception_test(e)

    # check the BackendError
    from Ganga.Core.exceptions import BackendError
    err = BackendError("TestBackend", test_str)
    assert err.__str__().find("TestBackend") != -1
    assert err.__str__().find(test_str) != -1

    # check the InaccessibleObjectError
    from Ganga.Core.exceptions import InaccessibleObjectError, JobManagerError
    from Ganga.Core.GangaRepository import getRegistry
    err = InaccessibleObjectError(getRegistry('jobs').repository, 0, JobManagerError("My JobManagerError"))
    assert err.__str__().find("jobs") != -1
    assert err.__str__().find("#0") != -1
    assert err.__str__().find("My JobManagerError") != -1

    # check the RepositoryError
    from Ganga.Core.exceptions import RepositoryError
    from Ganga.Core.GangaRepository import getRegistry
    RepositoryError(getRegistry('jobs').repository, test_str)
