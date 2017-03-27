def pytest_addoption(parser):
    parser.addoption("--runexternals", action="store_true",
        help="run tests that depend on external services")
    parser.addoption("--keepRepo", action="store_true",
        help="should tests preseve the repo used after completing")

pytest_plugins = "Ganga.testlib.fixtures"
