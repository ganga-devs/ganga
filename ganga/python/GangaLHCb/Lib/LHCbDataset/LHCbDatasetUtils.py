#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

from Ganga.Utility.Config import getConfig, ConfigError
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def getCacheAge():

    maximum_cache_age = 10080 #a week in minutes
    try:
        config = getConfig('LHCb')
        age = int(config['maximum_cache_age'])
        if age and age >= 1:
            maximum_cache_age = age
    except ConfigError:
        pass
    except ValueError:
        logger.error('The maximum_cache_age set in the LHCb section of the ' \
                     'Ganga config is not valid')
    return maximum_cache_age

def replicaCache(names):
    from GangaLHCb.Lib.DIRAC.Dirac import Dirac
    from GangaLHCb.Lib.DIRAC.DiracUtils import result_ok
    cmd = 'result = DiracCommands.getReplicas(%s)' % names
    result = Dirac.execAPI(cmd)
    if not result_ok(result):
        logger.warning('LFC query did not return cleanly: %s' % str(result))
    return result

def collect_lhcb_filelist(lhcb_files):
  """Forms list of filenames if files is list or LHCbDataset"""
  filelist = []
  if not lhcb_files: return filelist
  if isinstance(lhcb_files,list) or isinstance(lhcb_files,GangaList):
    filelist += [f for f in lhcb_files]
  else:
    filelist += [f.name for f in lhcb_files.files]

  return filelist

def collect_lfn_filelist(ds):
    s=[]
    if not ds: return s
    for k in ds.files:
      if k.isLFN(): s.append(k.name)
    return s

def dataset_to_options_string(ds):
    s=''
    if not ds: return s
    s='EventSelector.Input   = {'
    for k in ds.files:
        s+='\n'
        s+=""" "DATAFILE='%s' %s",""" % (k.name, ds.datatype_string)
    #Delete the last , to be compatible with the new optiosn parser
    if s.endswith(","):
        s=s[:-1]
    s+="""\n};"""
    return s

def dataset_to_lfn_string(ds):
    s=''
    if not ds: return s
    for k in ds.files:
      if k.isLFN(): s += ' %s' % k.name
    return s

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
