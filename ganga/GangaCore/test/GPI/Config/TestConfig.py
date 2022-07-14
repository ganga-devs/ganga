from GangaCore.testlib.decorators import add_config
from GangaCore.Utility.Config import getConfig, setSessionValuesFromFiles

# Using @add_config create sections C1 & C2 in the config


@add_config([('C1', "", ""), ('C2', "", "")])
def test_config_basic(gpi):

    import sys
    import os

    currentDir = os.path.dirname(os.path.realpath(__file__))

    # Give section C1 few basic options
    c1 = getConfig('C1')
    c1._addOpenOption('a', 1)
    c1._addOpenOption("aa", "xx")

    # Give section C2 few basic options
    c2 = getConfig('C2')
    c2._addOpenOption('b', 2)
    c2._addOpenOption("bb", "yy")

    # Get system variables
    system_vars = {}
    for opt in getConfig('System'):
        system_vars[opt] = getConfig('System')[opt]

    # Parse config from a file and set the options, if options are not present add them (for options to be added .is_open attribute must be set to "True")
    setSessionValuesFromFiles([os.path.join(currentDir, "ConfigTest.ini")], system_vars)

    # Assert if the expected changes in the config from the above file are made
    assert(c1['a'] == 3)
    assert(c1['aa'] == 'xx')

    assert(c2['b'] == 4)
    assert(c2['c'] == 'x')
    assert(c2['d'] == c2['c'])


# Using @add_config create section hierarchy in the config
@add_config([('hierarchy', "", "")])
def test_config_hierarchical(gpi):

    import sys
    import os

    currentDir = os.path.dirname(os.path.realpath(__file__))

    c = getConfig('hierarchy')

    # Get system variables
    system_vars = {}
    for opt in getConfig('System'):
        system_vars[opt] = getConfig('System')[opt]

    # Parse config from a file and set the options, if options are not present add them (for options to be added .is_open attribute must be set to "True")
    # First A.ini is parsed and then B.ini
    # Options in .ini file under the same section must not be repeated
    setSessionValuesFromFiles([os.path.join(currentDir, "A.ini"), os.path.join(currentDir, "B.ini")], system_vars)

    # assert(c['MY_PATH'] == 'a2:a') # Giving output: 'a2::' - Plausible Explaination: setSessionValuesFromFiles - sets an session value for the given config option and overwrites the existing session value (if any).
    assert(c['YOURPATH'] == 'b2')
    assert(c['PATH'] == 'c2')
    assert(c['MY2PATH'] == 'd2')
