################################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: PACKAGE.py,v 1.4 2008-10-23 13:00:39 ctan Exp $
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

external_packages = {
   'pyqt' : { 'version' : '3.18.1_python2.5', 'PYTHONPATH' : 'lib/python2.5/site-packages', 'LD_LIBRARY_PATH' : 'lib' },
   'Python' : { 'version' : '2.5', 'LD_LIBRARY_PATH' : 'lib', 'PATH' : 'bin' }
}

from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup(external_packages)

def standardSetup(setup=setup):
    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')
        setup.prependPath(p,'PATH')

#a hack for GUI in order to remove the environment variables (to avoid propagation to subshells)
def removeSetup(setup=setup):
    def remove_path(name,var):
        import os
        path = setup.getPackagePath2(name,var)
        if path:
            envList = filter( bool, os.environ[var].replace(' ','').split(':'))
            try:
               envList.remove( path )
            except ValueError, x:
               print "%s\nError removing %s from the %s environment variable." % (x, path, var)
            os.environ[var] = ':'.join( envList )
            
    for p in setup.packages:
        remove_path(p,'PYTHONPATH')
        remove_path(p,'LD_LIBRARY_PATH')
        remove_path(p,'PATH')
