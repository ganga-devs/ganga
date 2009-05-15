#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import Ganga.Utility.Config
import Ganga.Utility.logging
from Ganga.Core import ApplicationConfigurationError

logger = Ganga.Utility.logging.getLogger()
config = Ganga.Utility.Config.getConfig('DIRAC')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def getDaVinciVersion(rootversion=None):
    """Find the DaVinci version corresponding to a ROOT version"""
    
    rootversiondict = config['RootVersions']    
    rootversions = rootversiondict.keys()
    rootversions.sort()
    
    if 0==len(rootversions):
        msg = "No ROOT versions are defined in the [DIRAC][ROOTVersions] " \
              "part of the configuration. Please consult LHCb specific " \
              "Ganga manual to fix this at a local site."
        raise ApplicationConfigurationError("", msg)

    if None==rootversion:
        version = rootversions[-1]
        logger.info('No ROOT version specified. Will use %s which is the ' \
                    'latest one possible.' % version)
        return rootversiondict[version]
    
    try:
        version=rootversiondict[rootversion]
    except KeyError:
        msg = 'ROOT version %s is not available to use with DIRAC backend. ' \
              'Please pick a version from the following list and retry the ' \
              'submission of the job %s.' \
              % (rootversion,str(listRootVersions()))
        raise ApplicationConfigurationError("", msg)

    return version

def listRootVersions():
    rootversiondict=config['RootVersions']
    
    rootversions=rootversiondict.keys()
    rootversions.sort()

    return rootversions

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
