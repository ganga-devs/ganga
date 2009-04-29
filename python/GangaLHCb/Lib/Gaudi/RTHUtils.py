#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
import os
import tempfile
from Ganga.Core import ApplicationConfigurationError
from GangaLHCb.Lib.Dirac.DiracWrapper import diracwrapper
import Ganga.Utility.Config
from Ganga.Utility.files import expandfilename
from Ganga.GPIDev.Lib.File import FileBuffer, File
import Ganga.Utility.logging
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *

logger = Ganga.Utility.logging.getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

def jobid_as_string(job):
  jstr=''
  if job.master: jstr=str(job.master.id)+os.sep+str(job.id)
  else: jstr=str(job.id)
  return jstr

def gen_catalog(dataset,site):
  lfns = collect_lfn_filelist(dataset)
  depth = dataset.depth
  tmp_xml = tempfile.NamedTemporaryFile(suffix='.xml')
  #cmd = "os.system('dirac-lhcb-generate-catalog -d %d -n %s -f %s %s')" \
  #          % (depth,site,tmp_xml.name,lfns)
  cmd = '''
from DIRAC.LHCbSystem.Utilities.AncestorFiles import getAncestorFiles
from DIRAC.Interfaces.API.Dirac import *
lfns = %s
result = getAncestorFiles(lfns,%d)
if not result: sys.exit(2)
if not result[\'OK\']: sys.exit(2)
lfns = result[\'Value\']
dirac.getInputDataCatalog(lfns,\'%s\',\'%s\')
''' % (str(lfns),depth,site,tmp_xml.name)

  dw = diracwrapper(cmd)
  xml_catalog = tmp_xml.read()
  if dw.returnCode != 0 or not xml_catalog:    
    msg = "Error getting PFN's from LFN's (couldn't build catalog)"
    logger.error(msg)
    raise ApplicationConfigurationError(None,msg)

  tmp_xml.close()
  return xml_catalog

def get_master_input_sandbox(job,extra):
    sandbox = job.inputsandbox[:]
    sandbox += extra.master_input_files[:]
    buffers = extra.master_input_buffers
    sandbox += [FileBuffer(n,s) for (n,s) in buffers.items()]
    logger.debug("Master input sandbox: %s",str(sandbox))
    return sandbox

def get_input_sandbox(extra):
     sandbox = []
     sandbox += extra.input_files[:]
     sandbox += [FileBuffer(n,s) for (n,s) in extra.input_buffers.items()]
     logger.debug("Input sandbox: %s",str(sandbox))
     return sandbox

def is_gaudi_child(app):
    if app.__class__.__name__ == 'Gaudi' \
           or type(app).__bases__[0].__name__ == 'Gaudi':
        return True
    return False

def create_runscript(app,outputdata,job):

  config = Ganga.Utility.Config.getConfig('LHCb')
  which = 'GaudiPython'
  opts = None
  if is_gaudi_child(app):
      which = 'Gaudi'
      opts = 'options.pkl'
  
  jstr = jobid_as_string(job)
  appname = app.get_gaudi_appname()
  script =  "#!/usr/bin/env python\n\nimport os,sys\n\n"
  script += 'data_output = %s\n' % outputdata
  script += 'xml_cat = \'%s\'\n' % 'catalog.xml'
  script += 'data_opts = \'data.opts\'\n'
  script += 'opts = \'%s\'\n' % opts
  script += 'project_opts = \'%s\'\n' % app.setupProjectOptions
  script += 'app = \'%s\'\n' % appname
  script += 'app_upper = \'%s\'\n' % appname.upper()
  script += 'version = \'%s\'\n' % app.version
  script += 'package = \'%s\'\n' % app.package
  script += "job_output_dir = '%s/%s/outputdata'\n"%(config['DataOutput'],jstr)
  script += 'cp = \'%s\'\n' % config['cp_cmd']
  script += 'mkdir = \'%s\'\n' % config['mkdir_cmd']
  
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
import os
f=os.popen('which SetupProject.sh')
setup_script=f.read()[:-1]
f.close()
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
        
# add lib subdir in case user supplied shared libs where copied to pwd/lib
os.environ['LD_LIBRARY_PATH'] = '.:%s/lib:%s\' %(os.getcwd(),
                                                 os.environ['LD_LIBRARY_PATH'])
                                                 
#run
sys.stdout.flush()
os.environ['PYTHONPATH'] = '%s/python:%s' % (os.getcwd(),
                                             os.environ['PYTHONPATH'])

"""
  if which is 'GaudiPython':
    script += 'cmdline = \"python ./gaudiPythonwrapper.py\"\n'
  else:
    script += 'cmdline = \"%s/scripts/gaudirun.py %s data.opts\" % '
    script += "(os.environ['GAUDIROOT'],opts)\n"

  script += """
# run command
os.system(cmdline)

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

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
