from GangaCore.testlib.decorators import add_config
from GangaCore.Utility.Config import getConfig

# Case 1: Config Section exists & given option also exists -> Outcome expected: Should change the value of the option.
@add_config([('Configuration', 'user', 'test')])
def test_add_config_1(gpi):
    c = getConfig('Configuration')
    assert c['user'] == 'test'

# Case 2: Config Section exists BUT (.is_open=False) & given option does NOT exists -> Outcome expected: Should not add option to the config.
@add_config([('Configuration', 'TestOption', 'TestValue')])
def test_add_config_2(gpi):
    try:
        c = getConfig('Configuration')
        c['TestOption'] == 'TestValue'
        assert False, "Should throw an Error"
    except:
        assert True
    
# Case 3: Config Section does NOT exists -> Outcome expected: Should create a config section with (.is_open=True) & add option to it.
@add_config([('TestSection', 'TestOption', 'TestValue')])
def test_add_config_3(gpi):
    c = getConfig('TestSection')
    assert c['TestOption'] == 'TestValue'

#TODO Add test case with Config Section (.is_open=True) & option does NOT exist.