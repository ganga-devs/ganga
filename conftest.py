import pytest

def pytest_addoption(parser):
    parser.addoption("--runexternals", action="store_true",
        help="run tests that depend on external services")
    parser.addoption("--keepRepo", action="store_true",
        help="should tests preseve the repo used after completing")
    parser.addoption("--testLHCb", action="store_true",
        help="should we try to use an LHCb proxy for testing")

def pytest_configure(config):
    config.addinivalue_line("markers", "externals: mark test as having external dependencies")

def pytest_collection_modifyitems(config, items):
    if config.getoption("--runexternals"):
        # --runexternals given in cli: do not skip tests with external dependencies
        return
    skip_externals = pytest.mark.skip(reason="need --runexternals option to run")
    for item in items:
        if "externals" in item.keywords:
            item.add_marker(skip_externals)

pytest_plugins = "ganga.GangaCore.testlib.fixtures"
