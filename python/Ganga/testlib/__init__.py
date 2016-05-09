import pytest

external = pytest.mark.skipif(
    not pytest.config.getoption("--runexternals"),
    reason="need --runexternals option to run external tests"
)