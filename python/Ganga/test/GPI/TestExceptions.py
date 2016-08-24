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
        assert test_str in str(err_obj)

    err_list = ["GangaException", "ApplicationConfigurationError", "ApplicationPrepareError",
                "IncompleteJobSubmissionError", "IncompleteKillError", "JobManagerError", "GangaAttributeError",
                "GangaValueError", "GangaIOError", "SplitterError", "ProtectedAttributeError", "ReadOnlyObjectError",
                "TypeMismatchError", "SchemaError", "SchemaVersionError"]

    for e in err_list:
        exception_test(e)

    # check the BackendError
    from Ganga.Core.exceptions import BackendError
    err = BackendError("TestBackend", test_str)
    assert "TestBackend" in str(err)
    assert test_str in str(err)

    # check the InaccessibleObjectError
    from Ganga.Core.exceptions import InaccessibleObjectError, JobManagerError
    from Ganga.Core.GangaRepository import getRegistry
    err = InaccessibleObjectError(getRegistry('jobs').repository, 0, JobManagerError("My JobManagerError"))
    assert "jobs" in str(err)
    assert "#0" in str(err)
    assert "My JobManagerError" in str(err)

    # check the RepositoryError
    from Ganga.Core.exceptions import RepositoryError
    from Ganga.Core.GangaRepository import getRegistry
    RepositoryError(getRegistry('jobs').repository, test_str)

    # Construct another to check the except clause in the exception is called
    RepositoryError(getRegistry('jobs').repository, test_str)
