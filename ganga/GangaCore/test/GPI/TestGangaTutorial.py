from GangaCore.testlib.decorators import add_config
import os
import pytest

@pytest.fixture
def mock_env_ganga_config_path(monkeypatch):
    monkeypatch.setenv("GANGA_CONFIG_PATH", "GangaTutorial/Tutorial.ini")

# @pytest.fixture
# def mock_env_missing(monkeypatch):
#     monkeypatch.delenv("GANGA_CONFIG_PATH", raising=False)

def test_ganga_tutorial(mock_env_ganga_config_path, gpi):
    j = gpi.Job()
    j.application = gpi.PrimeFactorizer(number=123456)
    j.inputdata = gpi.PrimeTableDataset(table_id_lower=1, table_id_upper=1)
    j.submit()
    assert j.status in ['submitted','running','completed']

def test_ganga_tutorial_subjobs(mock_env_ganga_config_path, gpi):
    j = gpi.Job()
    j.splitter = gpi.PrimeFactorizerSplitter(numsubjobs=2)
    j.application = gpi.PrimeFactorizer(number=123456)
    j.inputdata = gpi.PrimeTableDataset(table_id_lower=1, table_id_upper=2)
    j.submit()
    assert len(j.subjobs) == 2
    assert j.status in ['submitted', 'running', 'completed']
    for sj in j.subjobs:
        assert sj.status in ['submitted', 'running', 'completed']
