################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.2 2008-08-21 12:35:21 hclee Exp $
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""
external_packages = {
   #'matplotlib' : {'version' : '0.99.0', 'PYTHONPATH':'lib/python2.5/site-packages'},
   #'numpy'      : {'version' : '1.3.0', 'PYTHONPATH':'lib/python2.5/site-packages', 'PATH' : 'bin'},
   #'pyqt'       : {'version' : '3.18.1_python2.5', 'PYTHONPATH':'lib/python2.5/site-packages', 'LD_LIBRARY_PATH' :'lib'}
   }

from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup(external_packages)

def standardSetup(setup=setup):
    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')
        setup.prependPath(p,'PATH')
