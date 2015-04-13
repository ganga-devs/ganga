################################################################################
# Ganga Project. http://cern.ch/ganga
#
################################################################################

""" Refer to Ganga/PACKAGE.py for details on the purpose of this module.
"""

external_packages = { }
from Ganga.Utility.Setup import PackageSetup

setup = PackageSetup(external_packages)

def standardSetup(setup=setup):
    import Ganga.Utility.Setup
#    Ganga.Utility.Setup.setPlatform('slc3_gcc323')
    
    for p in setup.packages:
        setup.prependPath(p,'PYTHONPATH')
        setup.prependPath(p,'PATH')
        setup.prependPath(p,'LD_LIBRARY_PATH')

from Ganga.GPIDev.Lib.Registry.JobRegistry import config as display_config
display_config.overrideDefaultValue( 'jobs_columns', ('fqid', 'status', 'name', 'subjobs', 'application', 'backend', 'backend.actualCE', 'backend.extraInfo', 'comment') )
#display_config.setConfigOption( 'jobs_columns', ('fqid', 'status', 'name', 'subjobs', 'application', 'backend', 'backend.actualCE', 'backend.extraInfo', 'comment') )
display_config.overrideDefaultValue( 'jobs_columns_functions', {'comment': 'lambda j: j.comment', 'backend.extraInfo': 'lambda j : j.backend.extraInfo ', 'subjobs': 'lambda j: len(j.subjobs)', 'backend.actualCE': 'lambda j:j.backend.actualCE', 'application': 'lambda j: j.application._name', 'backend': 'lambda j:j.backend._name'} )
#display_config.setConfigOption( 'jobs_columns_functions',  {'comment': 'lambda j: j.comment', 'backend.extraInfo': 'lambda j : j.backend.extraInfo ', 'subjobs': 'lambda j: len(j.subjobs)', 'backend.actualCE': 'lambda j:j.backend.actualCE', 'application': 'lambda j: j.application._name', 'backend': 'lambda j:j.backend._name'} )
display_config.overrideDefaultValue('jobs_columns_width', {'fqid': 8, 'status': 10, 'name': 10, 'application': 15, 'backend.extraInfo': 30, 'subjobs': 8, 'backend.actualCE': 17, 'comment': 20, 'backend': 15} )
#display_config.setConfigOption( 'jobs_columns_width', {'fqid': 8, 'status': 10, 'name': 10, 'application': 15, 'backend.extraInfo': 30, 'subjobs': 8, 'backend.actualCE': 17, 'comment': 20, 'backend': 15} )

