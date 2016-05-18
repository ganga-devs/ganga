def pytest_addoption(parser):
    parser.addoption("--runexternals", action="store_true",
        help="run tests that depend on external services")

