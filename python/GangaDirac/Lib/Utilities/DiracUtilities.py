from Ganga.Utility.Config import getConfig
from Ganga.Core.exceptions import GangaException
import os


def getDiracEnv():
    with open(getConfig('DIRAC')['DiracEnvFile'],'r') as env_file:
        return dict((tuple(line.strip().split('=',1)) for line in env_file.readlines() if len(line.strip().split('=',1)) == 2))
    return {}

def getDiracCommandIncludes():
    default_includes = ''
    for fname in getConfig('DIRAC')['DiracCommandFiles']:
        if not os.path.exists(fname):
            raise GangaException("Specified Dirac command file '%s' does not exist." % fname )
        f=open(fname, 'r')
        default_includes += f.read() + '\n'
        f.close()
    return default_includes
