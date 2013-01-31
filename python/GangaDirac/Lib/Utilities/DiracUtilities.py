from Ganga.Utility.Config import getConfig

def getDiracEnv():
    with open(getConfig('DIRAC')['DiracEnvFile'],'r') as env_file:
        return dict((tuple(line.strip().split('=',1)) for line in env_file.readlines() if len(line.strip().split('=',1)) == 2))
    return {}
