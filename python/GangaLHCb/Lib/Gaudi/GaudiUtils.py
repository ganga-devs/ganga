#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import os.path
import pprint, tempfile
from Ganga.Utility.Shell import Shell
from Ganga.Utility.files import fullpath
from Ganga.Utility.util import unique
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def available_apps():
  return ["Gauss", "Boole", "Brunel", "DaVinci", "Moore", "Vetra",
          "Panoptes", "Gaudi","Erasmus"]

def available_packs(appname):
  packs={'Gauss'   : 'Sim',
         'Boole'   : 'Digi',
         'Brunel'  : 'Rec',
         'DaVinci' : 'Phys',
         'Moore'   : 'Hlt',
         'Vetra'   : 'Tell1',
         'Panoptes': 'Rich',
         'Bender'  : 'Phys',
         'Erasmus' : ''}
  return packs[appname]

def available_versions(appname):
  """Provide a list of the available Gaudi application versions"""
  
  s = Shell()
  tmp = tempfile.NamedTemporaryFile(suffix='.log')
  command = 'SetupProject.sh --ask %s' % appname
  rc,output,m=s.cmd1("echo 'q\n' | %s >& %s; echo" % (command,tmp.name))
  output = tmp.read()
  tmp.close()
  versions = output[output.rfind('(')+1:output.rfind('q[uit]')].split()
  return versions

def guess_version(appname):
  """Guess the default Gaudi application version"""
  
  s = Shell()
  tmp = tempfile.NamedTemporaryFile(suffix='.log')
  command = 'SetupProject.sh --ask %s' % appname
  rc,output,m=s.cmd1("echo 'q\n' | %s >& %s; echo" % (command,tmp.name))
  output = tmp.read()
  tmp.close()
  version = output[output.rfind('[')+1:output.rfind(']')]
  return version
    
def get_user_platform(env=os.environ):
  if env.has_key('CMTCONFIG'):
    return env['CMTCONFIG']
  else:
    msg = '"CMTCONFIG" not set. Cannot determine the platform you want to use'
    logger.info(msg)
  return ''

def update_cmtproject_path(user_release_area,env=os.environ):

  if user_release_area:
    if env.has_key('CMTPROJECTPATH'):
      cmtpp=env['CMTPROJECTPATH'].split(':')
      if cmtpp[0] != user_release_area:
        cmtpp = [user_release_area] + cmtpp
        env['CMTPROJECTPATH']=':'.join(cmtpp)

def get_user_dlls(appname,version,user_release_area,platform,shell):

  user_ra = user_release_area
  update_cmtproject_path(user_release_area)
  full_user_ra = fullpath(user_ra) # expand any symbolic links

  # Work our way through the CMTPROJECTPATH until we find a cmt directory
  if not shell.env.has_key('CMTPROJECTPATH'): return [], [], []
  projectdirs = shell.env['CMTPROJECTPATH'].split(os.pathsep)
  appveruser = os.path.join(appname + '_' + version,'cmt')
  appverrelease = os.path.join(appname.upper(),appname.upper() + '_' + version,
                               'cmt')

  for projectdir in projectdirs:
    dir = fullpath(os.path.join(projectdir,appveruser))
    logger.debug('Looking for projectdir %s' % dir)
    if os.path.exists(dir):
      break
    dir = fullpath(os.path.join(projectdir,appverrelease))
    logger.debug('Looking for projectdir %s' % dir)
    if os.path.exists(dir):
      break

  logger.debug('Using the CMT directory %s for identifying projects' % dir)
  rc, showProj, m = shell.cmd1('cd ' + dir +';cmt show projects', 
                               capture_stderr=True)

  logger.debug(showProj)

  libs=[]
  merged_pys = []
  subdir_pys = {}
  project_areas = []
  py_project_areas = []

  for line in showProj.split('\n'):
    for entry in line.split():
      if entry.startswith(user_ra) or entry.startswith(full_user_ra):
        tmp = entry.rstrip('\)')
        libpath = fullpath(os.path.join(tmp,'InstallArea',platform,'lib'))
        logger.debug(libpath)
        project_areas.append(libpath)
        pypath = fullpath(os.path.join(tmp,'InstallArea','python'))
        logger.debug(pypath)
        py_project_areas.append(pypath)
        pypath = fullpath(os.path.join(tmp,'InstallArea',platform,'python'))
        logger.debug(pypath)
        py_project_areas.append(pypath)

  # savannah 47793 (remove multiple copies of the same areas)
  project_areas = unique(project_areas)
  py_project_areas = unique(py_project_areas)

  ld_lib_path = []
  if shell.env.has_key('LD_LIBRARY_PATH'):
    ld_lib_path = shell.env['LD_LIBRARY_PATH'].split(':')
  project_areas_dict = {}
  for area in project_areas:
    if area in ld_lib_path:
      project_areas_dict[area] = ld_lib_path.index(area)
    else:
      project_areas_dict[area] = 666
  from operator import itemgetter
  sorted_project_areas = []
  for item in sorted(project_areas_dict.items(),key=itemgetter(1)):
    sorted_project_areas.append(item[0])
  
  lib_names = []  
  for libpath in sorted_project_areas:
    if os.path.exists(libpath):
      for f in os.listdir(libpath):
        if lib_names.count(f) > 0: continue
        fpath = os.path.join(libpath,f)
        if os.path.exists(fpath):
          lib_names.append(f)
          libs.append(fpath)
        else:
          logger.warning("File %s in %s does not exist. Skipping...",
                         str(f),str(libpath))

  for pypath in py_project_areas:
    if os.path.exists( pypath):
      for f in os.listdir( pypath):
        confDB_path = os.path.join( pypath, f)
        if confDB_path.endswith( '.py'):
          if os.path.exists( confDB_path):
            merged_pys.append( confDB_path)
          else:
            logger.warning("File %s in %s does not exist. Skipping...",
                           str(f),str(confDB_path))
        elif os.path.isdir(confDB_path):
          pyfiles = []
          for g in os.listdir(confDB_path):
            file_path = os.path.join(confDB_path, g)
            if (file_path.endswith('.py')):
              if os.path.exists(file_path):
                pyfiles.append(file_path)
              else:
                logger.warning("File %s in %s does not exist. Skipping...",
                               str(g),str(f))
          subdir_pys[ f] = pyfiles
                    
  logger.debug("%s",pprint.pformat( libs))
  logger.debug("%s",pprint.pformat( merged_pys))
  logger.debug("%s",pprint.pformat( subdir_pys))

  return libs, merged_pys, subdir_pys

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
