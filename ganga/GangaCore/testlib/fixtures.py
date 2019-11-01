

import os
import shutil

import pytest

from GangaCore.testlib.GangaUnitTest import start_ganga, stop_ganga, _getGangaPath, ganga_test_dir_name

def gangadir(request):
    """
    Return the gangdir that should be used for a given test.

    Args:
        request: A pytest FixtureRequest object

    Returns:
        str: the name of the gangadir directory to use

    """
    return os.path.join(_getGangaPath(), ganga_test_dir_name, request.module.__name__)


@pytest.yield_fixture(scope='module')
def wipe_gangadir(request):
    """
    Wipe the gangadir for the test at the beginning of the test
    """
    shutil.rmtree(gangadir(request), ignore_errors=True)
    yield


@pytest.yield_fixture(scope='function')
def gpi(request, wipe_gangadir):
    """
    Provide the ganga GPI and runtime to the test.

    This allows full Ganga integration tests. The GPI namespace is
    available via the ``ganga`` fixture argument or via an import of
    ``GangaCore.GPI`` inside the function as usual.
    """
    config_values = getattr(request._pyfuncitem._obj, '_config_values', {})
    config_values = [(k[0], k[1], v) for k, v in list(config_values.items())]  # Convert from dict to a list of tuples
#    testLHCb = request.config.getoption("--testLHCb")
    start_ganga(gangadir(request), extra_opts=config_values)

    import GangaCore.GPI
    yield GangaCore.GPI

    stop_ganga()
