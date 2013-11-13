import os
from Ganga.Utility.Config  import getConfig
from Ganga.Core.exceptions import GangaException

## Cache
# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
DIRAC_ENV={}
DIRAC_INCLUDE=''

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def getDiracEnv(force=False):
    global DIRAC_ENV
    if DIRAC_ENV == {} or force:
        with open(getConfig('DIRAC')['DiracEnvFile'],'r') as env_file:
            DIRAC_ENV = dict((tuple(line.strip().split('=',1)) for line in env_file.readlines() if len(line.strip().split('=',1)) == 2))
    return DIRAC_ENV

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def getDiracCommandIncludes(force=False):
    global DIRAC_INCLUDE
    if DIRAC_INCLUDE == '' or force:
        for fname in getConfig('DIRAC')['DiracCommandFiles']:
            if not os.path.exists(fname):
                raise GangaException("Specified Dirac command file '%s' does not exist." % fname )
            with open(fname, 'r') as inc_file:
                DIRAC_INCLUDE += inc_file.read() + '\n'

    return DIRAC_INCLUDE

# /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
def getValidDiracFiles(job, names=None):
    from GangaDirac.Lib.Files.DiracFile import DiracFile
    if job.subjobs:
        for sj in job.subjobs:
            for df in (f for f in sj.outputfiles if isinstance(f, DiracFile)):
                if df.subfiles:
                    for valid_sf in (sf for sf in df.subfiles if sf.lfn!='' and (names is None or sf.namePattern in names)):
                        yield valid_sf
                else:
                    if df.lfn!='' and (names is None or df.namePattern in names):
                        yield df
    else:
        for df in (f for f in job.outputfiles if isinstance(f, DiracFile)):
            if df.subfiles:
                for valid_sf in (sf for sf in df.subfiles if sf.lfn!='' and (names is None or sf.namePattern in names)):
                    yield valid_sf
            else:
                if df.lfn!='' and (names is None or df.namePattern in names):
                    yield df
