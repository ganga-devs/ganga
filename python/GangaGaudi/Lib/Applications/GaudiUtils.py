#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import os.path
import pprint, tempfile
from Ganga.Utility.Shell import Shell
from Ganga.Utility.files import fullpath
from Ganga.Utility.util import unique
import Ganga.Utility.logging
import Ganga.Utility.Config
from Ganga.Core import GangaException

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
import time
import signal

def shellEnv_cmd(cmd, env=None, cwd=None, timeout=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
  '''Tool for running a shell command within a specific environment. timeout is in seconds. If stdout or
  stderr are file objects then None is returned for the stdout and stderr part of the return tuple'''
  startTime=time.time()

  pipe = subprocess.Popen( cmd,
                           shell=True,
                           env=env,
                           cwd=cwd,
                           stdout=stdout,
                           stderr=stderr,
                           preexec_fn=os.setsid)
  
  while pipe.poll() is None:
    if timeout and ( time.time() - startTime ) >= timeout:
      os.killpg(pipe.pid, signal.SIGTERM) # Send the signal to all the process groups
      pipe.wait()
      preambleFindString = 'INT TERM EXIT;' #catch calls coming from the update function below
      preambleEndLocation = cmd.find(preambleFindString)
      if preambleEndLocation <0:
        msg = 'Timeout issuing command: "%s"' % cmd
      else:
        msg = 'Timeout issuing command: "%s"' % cmd[preambleEndLocation+len(preambleFindString):]
      return 1, msg, msg

    time.sleep(0.5)

  stdout, stderr  = pipe.communicate()# blocking command so poll until know complete, that way can escape after timeout
  return pipe.returncode, stdout, stderr

def shellEnvUpdate_cmd(cmd, env=os.environ, cwd=None, timeout=None, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
  import tempfile, pickle
  f = tempfile.NamedTemporaryFile(mode='wb')
  fname = f.name
  f.close()

  envdump  = 'import os, pickle;'
  envdump += 'f=open(\'%s\',\'wb\');' % fname
  envdump += 'pickle.dump(os.environ,f);'
  envdump += 'f.close()'
  envdumpcommand = """trap \"/usr/bin/env python -c \\\"%s\\\" \" INT TERM EXIT;""" % envdump
  cmd = envdumpcommand + cmd

  rc, stdout, stderr = shellEnv_cmd( cmd,
                                     env=env,
                                     cwd=cwd,
                                     timeout=timeout,
                                     stdout=stdout,
                                     stderr=stderr )

  # if the file doesn't exist then didn't get to the env update stuff as error somewhere
  if not os.path.exists(fname):
    logger.warning('Couln\'t find the environment update file so environment not updated.')
    return rc, stdout, stderr

  f = open(fname,'rb')
  env=env.update(pickle.load(f))
  f.close()
  os.system('rm -f %s' % fname)
  
  return rc, stdout, stderr

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
def fillPackedSandbox(sandbox_files,destination):
  """Put all sandbox_files into tarball called name and write it into to the destination.
  Arguments:
  'sandbox_files': a list of File or FileBuffer objects.
  'destination': a string representing the destination filename
  Return: a list containing a path to the tarball
  """
  if not sandbox_files:
    return []

  ## Generalised version from Ganga/Core/Sandbox/Sandbox.py
  
  import tarfile
  import stat
  
  #tf = tarfile.open(destination,"w:gz")

  ## "a" = append with no compression
  ## creates file if doesn't exist
  ## cant append to a compressed tar archive so must compress later
  dir, filename = os.path.split(destination)
  if not os.path.isdir(dir):
    os.makedirs(dir)
  tf = tarfile.open(destination,"a")
  tf.dereference=True  #  --not needed in Windows
  
  for f in sandbox_files:
    try:
      contents=f.getContents()   # is it FileBuffer?
      
    except AttributeError:         # File
      try:
        fileobj = file(f.name)
      except:
        raise GangaException("File %s does not exist."%f.name) 
      tinfo = tf.gettarinfo(f.name,os.path.join(f.subdir,os.path.basename(f.name)))
      
    else:                          # FileBuffer
      from StringIO import StringIO
      fileobj = StringIO(contents)
      
      tinfo = tarfile.TarInfo()
      tinfo.name = os.path.join(f.subdir,os.path.basename(f.name))
      import time
      tinfo.mtime = time.time()
      tinfo.size = fileobj.len
      
    if f.isExecutable():
      tinfo.mode=tinfo.mode|stat.S_IXUSR
    tf.addfile(tinfo,fileobj)
      
  tf.close()
      
  return [destination]


def gzipFile(filename, outputfilename=None, removeOriginal=False):
  import gzip
  if not outputfilename:
    outputfilename = filename + '.gz'
  f_in = open(filename, 'rb')
  f_out = gzip.open(outputfilename, 'wb')
  f_out.writelines(f_in)
  f_out.close()
  f_in.close()
  if removeOriginal:
    os.system('rm -f %s'%filename)
