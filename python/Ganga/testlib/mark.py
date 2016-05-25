"""A set of useful decorators for testing"""

# System imports
import pytest

external = pytest.mark.skipif(
    not pytest.config.getoption("--runexternals"),
    reason="need --runexternals option to run external tests"
)
"""decorator: Marks the test as depending on external services"""

nocoverage = pytest.mark.skipif(
    pytest.config.getoption("--cov"),
    reason="exclude this test if running with coverage"
)
"""decorator: Excludes this test if running with coverage

Some tests (generally the thread based ones) hang when run with coverage. If marked, they will be disabled when running
with coverage switched on.
"""