""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

external_packages = {
        #TestOOB testing tool
        'PYTF' : {'version' : '1.6', 'noarch':True, 'PYTHONPATH':'.', 'PYTF_TOP_DIR':'.'},
        #coverage reporting tool
        'figleaf' : {'version' : '0.6', 'noarch':True, 'PYTHONPATH':'.'},
        #test externals
        'test-externals':{'version':'1.0', 'noarch':True, 'TEST_EXTERNAL_PATH':'.'},
#        #subprocess
#        'subprocess':{'version':'2.4.2', 'noarch':True, 'PYTHONPATH':'lib/python2.2/site-packages', 'maxHexVersion':'0x20400f0'}
        }

from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup(external_packages)

def standardSetup(setup=setup):
        for p in setup.packages:
                setup.prependPath(p,'PYTHONPATH')
                setup.prependPath(p,'LD_LIBRARY_PATH')
                setup.prependPath(p,'PATH')
                setup.setPath(p,'PYTF_TOP_DIR')
                setup.setPath(p,'TEST_EXTERNAL_PATH')

