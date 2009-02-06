#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Utility methods used by various classes in GangaLHCb.Lib.Gaudi.'''

__author__ = 'Greig A Cowan, Ulrik Egede, Andrew Maier, Mike Williams'
__date__ = "$Date: 2009-02-06 14:31:56 $"
__revision__ = "$Revision: 1.9 $"

import os
import os.path
import pprint
import sys
import tempfile
from GangaLHCb.Lib.LHCbDataset import LHCbDataset
from Ganga.Utility.Shell import Shell
from Ganga.Utility.files import expandfilename, fullpath
from Ganga.GPIDev.Lib.GangaList.GangaList import GangaList
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.logging

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def available_apps():
  return ["Gauss", "Boole", "Brunel", "DaVinci","Euler", "Moore", "Vetra",
          "Panoramix","Panoptes", "Gaudi", "Bender"]

def available_packs(appname):
  packs={'Gauss'   : 'Sim',
         'Boole'   : 'Digi',
         'Brunel'  : 'Rec',
         'DaVinci' : 'Phys',
         'Euler'   : 'Trg',
         'Moore'   : 'Hlt',
         'Vetra'   : 'Velo',
         'Panoptes': 'Rich',
         'Bender'  : 'Phys'}
  return packs[appname]

def available_versions(appname):
  """Provide a list of the available Gaudi application versions"""
  
  s = Shell()
  command = '${LHCBSCRIPTS}/SetupProject.sh --ask %s' % appname
  rc,output,m=s.cmd1("echo 'q\n' | %s" % command)
  versions = output[output.rfind('(')+1:output.rfind('q[uit]')].split()
  return versions

def guess_version(appname):
  """Guess the default Gaudi application version"""
  
  s = Shell()
  command = '${LHCBSCRIPTS}/SetupProject.sh --ask %s' % appname
  rc,output,m=s.cmd1("echo 'q\n' | %s" % command)
  version = output[output.rfind('[')+1:output.rfind(']')]
  return version

def check_gaudi_inputs(optsfile,appname):
  """Checks the validity of some of user's entries for Gaudi(Python) schema"""
  for fileitem in optsfile:
    fileitem.name = os.path.expanduser(fileitem.name)
    fileitem.name = os.path.normpath(fileitem.name)
    
  if appname is None:
    logger.error("The appname is not set. Cannot configure")
    msg = "The appname is not set. Cannot configure"
    raise ApplicationConfigurationError(None,msg)
    
  if appname not in available_apps():
    msg = "Unknown applications %s. Cannot configure." % appname
    logger.error()
    raise ApplicationConfigurationError(None,msg)
     

def gaudishell_setenv(gaudiapp):
    # generate shell script
    ver=gaudiapp.version
    pack=gaudiapp.appname 
    opts = gaudiapp.setupProjectOptions

    import tempfile
    fd=tempfile.NamedTemporaryFile()
    script = '#!/bin/sh\n'
    try:
        script +='User_release_area=%s; export User_release_area\n' % \
                  expandfilename(gaudiapp.user_release_area)
    except AttributeError:
        pass
    script +='. ${LHCBSCRIPTS}/SetupProject.sh %s %s %s\n'\
              % (opts, pack, ver)
    fd.write(script)
    fd.flush()
    logger.debug(script)

    s = Shell(setup=fd.name)
    logger.debug(pprint.pformat(s.env))
    
    return s

def collect_lhcb_filelist(lhcb_files):
  """Forms list of filenames if files is list or LHCbDataset"""
  filelist = []
  if not lhcb_files: return filelist
  if isinstance(lhcb_files,list) or isinstance(lhcb_files,GangaList):
    filelist += [f for f in lhcb_files]
  else:
    filelist += [f.name for f in lhcb_files.files]

  return filelist

def jobid_as_string(job):
  jstr=''
  # test is this is a subjobs or not
  if job.master: # it's a subjob
    jstr=str(job.master.id)+os.sep+str(job.id)
  else:
    jstr=str(job.id)
  return jstr

#def dataset_to_options_string(ds):
#    '''This creates a python options file for the input data.
#       Cannot use this at the moment due to genCatalog.'''
#    if not ds: return ''
#    s  = 'from Configurables import EventSelector\n'
#    s += 'sel = EventSelector()\n'
#    s += 'sel.Input = ['
#    for k in ds.files:
#        s+=""" "DATAFILE='%s' %s",""" % (k.name, ds.datatype_string)
#    if s.endswith(','): 
#        logger.debug('_dataset2optsstring: removing trailing comma')
#        s=s[:-1]
#    s += ']'
#    return s

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
    
def get_user_platform(env=os.environ):
  if env.has_key('CMTCONFIG'):
    return env['CMTCONFIG']
  else:
    msg = '"CMTCONFIG" not set. Cannot determine the platform you want to use'
    logger.info(msg)
  return ''

def gen_catalog(dataset,site):
  from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper
  lfns = dataset_to_lfn_string(dataset)
  depth = dataset.depth
  tmp_xml = tempfile.NamedTemporaryFile(suffix='.xml')
  cmd = "os.system('dirac-lhcb-generate-catalog -d %d -n %s -f %s %s')" \
            % (depth,site,tmp_xml.name,lfns)

  #logger.info('About to generate XML catalog, this may take a while...')
  if diracwrapper(cmd).returnCode != 0:
    msg = "Error getting PFN's from LFN's (couldn't build catalog)"
    logger.error()
    raise ApplicationConfigurationError(None,msg)

  return tmp_xml.read()

def create_lsf_runscript(app,appname,xml_catalog,package,opts,
                         user_release_area,outputdata,job,which):

  import Ganga.Utility.Config 
  config = Ganga.Utility.Config.getConfig('LHCb')
  version = app.version
  platform = app.platform
  app_upper = appname.upper()
  jstr = jobid_as_string(job)
  copy_cmd = config['cp_cmd']
  mkdir_cmd = config['mkdir_cmd']
  joboutdir = config['DataOutput']
  projectopts = app.setupProjectOptions
  
  script =  "#!/usr/bin/env python\n\nimport os,sys\n\n"
  script += 'user_release_area = \'%s\'\n' % user_release_area
  script += 'data_output = %s\n' % outputdata
  script += 'xml_cat = \'%s\'\n' % xml_catalog
  script += 'data_opts = \'dataopts.opts\'\n'
  script += 'opts = \'%s\'\n' % opts
  script += 'project_opts = \'%s\'\n' % projectopts
  script += 'app = \'%s\'\n' % appname
  script += 'app_upper = \'%s\'\n' % app_upper
  script += 'version = \'%s\'\n' % version
  script += 'package = \'%s\'\n' % package
  script += 'job_output_dir = \'%s/%s/outputdata\'\n' % (joboutdir,jstr)
  script += 'cp = \'%s\'\n' % copy_cmd
  script += 'mkdir = \'%s\'\n\n' % mkdir_cmd

  if opts:
    script += """# check that options file exists
if not os.path.exists(opts):
    opts = 'notavailable'
    os.environ['JOBOPTPATH'] = opts
else:
    os.environ['JOBOPTPATH'] = '%s/%s/%s_%s/%s/%s/%s/options/job.opts' \
                               % (os.environ[app + '_release_area'],app_upper,
                                  app_upper,version,package,app,version)
    print 'Using the master optionsfile:', opts
    sys.stdout.flush()
    
"""

  script+="""# check that SetupProject.sh script exists, then execute it    
setup_script = os.environ['LHCBSCRIPTS'] + '/SetupProject.sh'
if os.path.exists(setup_script):
    os.system('/usr/bin/env bash -c \"source %s %s %s %s && printenv > \
env.tmp\"' % (setup_script,project_opts,app,version))
    for line in open('env.tmp').readlines():
        varval = line.strip().split('=')
        os.environ[varval[0]] = ''.join(varval[1:])
        os.system('rm -f env.tmp')
else:
    print 'Could not find %s. Your job will probably fail.' % setup_script
    sys.stdout.flush()
    
# create an xml slice
if os.path.exists(data_opts) and os.path.exists(xml_cat):
    f = open(data_opts,'a')
    f.write('\\n')
    f.write('FileCatalog.Catalogs += { \\"xmlcatalog_file:%s\\" };' % xml_cat)
    f.close()
    
"""
  if which == 'Gaudi':
    script +="""
# add lib subdir in case user supplied shared libs where copied to pwd/lib
os.environ['LD_LIBRARY_PATH'] = '.:%s/lib:%s\' %(os.getcwd(),
                                                 os.environ['LD_LIBRARY_PATH'])

#run
sys.stdout.flush()
os.environ['PYTHONPATH'] = '%s/python:%s' % (os.getcwd(),
                                             os.environ['PYTHONPATH'])
cmd = '%s/scripts/gaudirun.py %s %s' % (os.environ['GAUDIROOT'],opts,data_opts)
os.system(cmd)
"""
  else:
    script += """
os.system('python ./gaudiPythonwrapper.py')
"""

  script+="""
# make output directory + cp files to it
if data_output:
    os.system('%s -p %s' % (mkdir,job_output_dir))
for f in data_output:
    cpval = os.system('%s %s %s/%s' % (cp,f,job_output_dir,f))
    print 'Copying %s to %s' % (f,job_output_dir)
    sys.stdout.flush()
    if cpval != 0:
        print 'WARNING:  Could not copy file %s to %s' % (f,job_output_dir)
        print 'WARNING:  File %s will be lost' % f
        sys.stdout.flush()
    # sneaky rm
    os.system('rm -f ' + f)
"""    
  return script

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

  for libpath in project_areas:
    if os.path.exists(libpath):
      for f in os.listdir(libpath):
        fpath = os.path.join(libpath,f)
        if os.path.exists( fpath):
          libs.append( fpath)
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
