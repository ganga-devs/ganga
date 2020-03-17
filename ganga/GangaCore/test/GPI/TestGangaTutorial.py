from GangaCore.testlib.decorators import add_config

@add_config([("Configuration", "RUNTIME_PATH", "GangaTutorial")])
def test_ganga_tutorial(gpi):
    j = gpi.Job()
    j.application = gpi.PrimeFactorizer(number=123456)
    j.inputdata = gpi.PrimeTableDataset(table_id_lower=1, table_id_upper=1)
    j.submit()
    assert j.status in ['submitted','running','completed']

@add_config([("Configuration", "RUNTIME_PATH", "GangaTutorial")])
def test_ganga_tutorial_subjobs(gpi):
    j = gpi.Job()
    j.splitter = gpi.PrimeFactorizerSplitter(numsubjobs=2)
    j.application = gpi.PrimeFactorizer(number=123456)
    j.inputdata = gpi.PrimeTableDataset(table_id_lower=1, table_id_upper=2)
    j.submit()
    assert len(j.subjobs) == 2
    assert j.status in ['submitted', 'running', 'completed']
    for sj in j.subjobs:
        assert sj.status in ['submitted', 'running', 'completed']
