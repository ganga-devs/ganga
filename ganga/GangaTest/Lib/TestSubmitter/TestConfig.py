from GangaCore.Utility.Config import makeConfig

# test configuration properties
test_config = makeConfig('TestConfig','testing stuff')
test_config.addOption('None_OPT', None, '')
test_config.addOption('Int_OPT', 1, '')
test_config.addOption('List_OPT', [1,2,3], '')
test_config.addOption('String_OPT' ,'dupa', '')
# there is also an Undefine_OPT which will be used in the test case
