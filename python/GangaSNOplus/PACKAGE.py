################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.1 2008-07-17 16:41:37 moscicki Exp $
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

external_packages = {
    #   'pyqt' : {'version' : '3.13_python234', 'PYTHONPATH':'lib/python2.3.4/site-packages', 'LD_LIBRARY_PATH' :'lib'},
    #   'Python' : {'version' : '2.3.4', 'PYTHONPATH':'lib/python2.3', 'LD_LIBRARY_PATH':'lib', 'PATH' : 'bin'} }
        }

from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup(external_packages)

def standardSetup(setup=setup):
    for p in setup.packages:
        pass
        #setup.prependPath(p,'PYTHONPATH')
        #setup.prependPath(p,'LD_LIBRARY_PATH')
        #setup.prependPath(p,'PATH')  
