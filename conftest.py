def pytest_addoption(parser):
    parser.addoption("--runexternals", action="store_true",
        help="run tests that depend on external services")
    parser.addoption("--keepRepo", action="store_true",
        help="should tests preseve the repo used after completing")
    parser.addoption("--testLHCb", action="store_true",
        help="should we try to use an LHCb proxy for testing")

pytest_plugins = "ganga.GangaCore.testlib.fixtures"
