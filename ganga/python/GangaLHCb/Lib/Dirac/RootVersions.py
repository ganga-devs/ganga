import Ganga.Utility.Config
config = Ganga.Utility.Config.getConfig('DIRAC')

import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()


def getDaVinciVersion(rootversion=None):
    """Find the DaVinci version corresponding to a ROOT version"""

    from Ganga.Core import ApplicationConfigurationError

    rootversiondict=config['RootVersions']
    
    rootversions=rootversiondict.keys()
    rootversions.sort()
    if 0==len(rootversions):
        raise ApplicationConfigurationError("", "'No ROOT versions are defined in the [DIRAC][ROOTVersions] part of the configuration. Please consult LHCb specific Ganga manual to fix this at a local site.")

    if None==rootversion:
        version = rootversions[-1]
        logger.info('No ROOT version specified. Will use %s which is the latest one possible.' % version)
        return rootversiondict[version]
    
    try:
        version=rootversiondict[rootversion]
    except KeyError:
        raise ApplicationConfigurationError("",'ROOT version %s is not available to use with DIRAC backend. Please pick a version from the following list and retry the submission of the job %s.' % (rootversion,str(listRootVersions())))

    return version

def listRootVersions():
    rootversiondict=config['RootVersions']
    
    rootversions=rootversiondict.keys()
    rootversions.sort()

    return rootversions
