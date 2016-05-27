from __future__ import print_function
from Ganga.Utility.Config import getConfig, setSessionValuesFromFiles

if __name__ == "__main__":

    import Ganga.Utility.logging

    Ganga.Utility.logging.config['Ganga.Utility.Config'] = 'DEBUG'
    Ganga.Utility.logging.bootstrap()

    print('Basic Test')
    import sys

    c1 = getConfig('C1')
    c1.setDefaultOptions({'a': 1, 'aa': 'xx'})

    c2 = getConfig('C2')
    c2['b'] = 2
    c2['bb'] = 'yy'

    print('path=', sys.path[0])

    setSessionValuesFromFiles([sys.path[0] + '/ConfigTest.ini'])

    print("C1")
    print(c1.getEffectiveOptions())
    print()
    print("C2")
    print(c2.getEffectiveOptions())

    assert(c1['a'] == 3)
    assert(c1['aa'] == 'xx')

    assert(c2['b'] == 4)
    assert(c2['c'] == 'x')
    assert(c2['d'] == c2['c'])

    print('Basic Test OK')
    print()
    print('Hierarchical Test')

    c = getConfig('hierarchy')

    setSessionValuesFromFiles([sys.path[0] + '/A.ini', sys.path[0] + '/B.ini'])

    print(c.getEffectiveOptions())

    assert(c['MY_PATH'] == 'a2:a')
    assert(c['YOURPATH'] == 'b2')
    assert(c['PATH'] == 'c2')
    assert(c['MY2_PATH'] == 'd2')

    print('Hierarchical Test OK')
