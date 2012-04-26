#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import os.path
import pprint, tempfile
from Ganga.Utility.Shell import Shell
from Ganga.Utility.files import fullpath
from Ganga.Utility.util import unique
import Ganga.Utility.logging
import Ganga.Utility.Config

logger = Ganga.Utility.logging.getLogger()
configGaudi = Ganga.Utility.Config.getConfig('GAUDI')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

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

def pyFileCollector(dir, file_list, subdir_dict, depth_cut, depth=0,zerodepth_pathlength=0):
  if zerodepth_pathlength==0: zerodepth_pathlength=len(dir)+1
  sub_pys=[]
  for item in os.listdir(dir):
    file_path = os.path.join(dir, item)
    if (file_path.endswith('.py')):
      if os.path.exists(file_path):
        if depth==0: file_list.append(file_path)
        else: sub_pys.append(file_path) 
      else:
        logger.warning("File %s in %s does not exist. Skipping...",str(item),str(dir))
    elif os.path.isdir(file_path) and not os.path.islink(file_path):
      if depth >=depth_cut: continue
      pyFileCollector(file_path,file_list,subdir_dict,depth_cut,depth+1,zerodepth_pathlength)
        
  if depth !=0:
    subdir_dict[dir[zerodepth_pathlength:]] = sub_pys


def get_user_dlls(appname,version,user_release_area,platform,env):

  user_ra = user_release_area
  update_cmtproject_path(user_release_area)
  full_user_ra = fullpath(user_ra) # expand any symbolic links

  # Work our way through the CMTPROJECTPATH until we find a cmt directory
  if not env.has_key('CMTPROJECTPATH'): return [], [], []
  projectdirs = env['CMTPROJECTPATH'].split(os.pathsep)
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
##   rc, showProj, m = shell.cmd1('cd ' + dir +';cmt show projects', 
##                                capture_stderr=True)
  rc, showProj, m = shellEnv_cmd('cmt show projects', 
                                 env,
                                 dir)

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
  if env.has_key('LD_LIBRARY_PATH'):
    ld_lib_path = env['LD_LIBRARY_PATH'].split(':')
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
      pyFileCollector(pypath, merged_pys, subdir_pys,configGaudi['pyFileCollectionDepth'])

  logger.debug("%s",pprint.pformat( libs))
  logger.debug("%s",pprint.pformat( merged_pys))
  logger.debug("%s",pprint.pformat( subdir_pys))

  return libs, merged_pys, subdir_pys

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import subprocess
def shellEnv_cmd(cmd, environ=None, cwdir=None):
    pipe = subprocess.Popen(cmd,
                            shell=True,
                            env=environ,
                            cwd=cwdir,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    result  = pipe.communicate()
    retcode = pipe.poll()
    return retcode, result[0], result[1]

def shellEnvUpdate_cmd(cmd, environ=os.environ, cwdir=None):
  import tempfile, pickle
  fname = tempfile.mkstemp()[1]
  os.system('rm -f %s' % fname)

  if not cmd.endswith(';'): cmd += ';'
  envdump  = 'import os, pickle;'
  envdump += 'f=open(\'%s\',\'w+b\');' % fname
  envdump += 'pickle.dump(os.environ,f);'
  envdump += 'f.close();'
  envdumpcommand = 'python -c \"%s\"' % envdump
  cmd += envdumpcommand
  
  pipe = subprocess.Popen(cmd,
                          shell=True,
                          env=environ,
                          cwd=cwdir,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE)
  result  = pipe.communicate()
  retcode = pipe.poll()

  f = open(fname,'r+b')
  environ=environ.update(pickle.load(f))
  f.close()
  os.system('rm -f %s' % fname)
  
  
  return retcode, result[0], result[1]
