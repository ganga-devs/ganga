import pytest

from GangaDirac.Lib.Utilities.DiracUtilities import getDiracEnv

from Ganga.testlib.GangaUnitTest import load_config_files, clear_config


@pytest.yield_fixture(scope='module', autouse=True)
def config_files():
    """
    Load the config files in a way similar to a full Ganga session
    NB: This is taken to be only within the context of __this__ file!
    """
    load_config_files()
    yield
    clear_config()


def test_dirac_env():
    env = getDiracEnv()
    assert any(key.startswith('DIRAC') for key in env)

