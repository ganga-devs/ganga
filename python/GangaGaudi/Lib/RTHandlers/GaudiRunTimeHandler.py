#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
import Ganga.Utility.Config 
#from RTHUtils import *
from GangaLHCb.Lib.LHCbDataset.LHCbDataset import *
from GangaLHCb.Lib.LHCbDataset.LHCbDatasetUtils import *
#from GangaLHCb.Lib.Gaudi.GaudiJobConfig import GaudiJobConfig
from GangaLHCb.Lib.LHCbDataset.OutputData import OutputData
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Lib.File import FileBuffer, File
from Ganga.Core import TypeMismatchError
from Ganga.Utility.util import unique
import shutil
from RunTimeHandlerUtils import sharedir_handler
logger = Ganga.Utility.logging.getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiRunTimeHandler(IRuntimeHandler):
    """This is the application runtime handler class for Gaudi applications 
    using the local, interactive and LSF backends."""
  
    def __init__(self):
        pass

    def master_prepare(self,app,appmasterconfig):
        ## Note EITHER the master inputsandbox OR the job.inputsandbox is added to
        ## the subjob inputsandbox depending if the jobmasterconfig object is present
        ## or not... Therefore combine the job.inputsandbox with appmasterconfig.
        job=app.getJobObject()
        ## catch errors from not preparing properly
        if not hasattr(app,'is_prepared') or app.is_prepared is None:
            logger.warning('Application is not prepared properly')
            raise GangaException(None,'Application not prepared properly')

        ## user added items from the interactive GPI
        inputsandbox=job.inputsandbox[:]
        outputsandbox=job.outputsandbox[:]

        ## inputsandbox files stored in share_dir from prepare method
        sharedir_handler(app, 'inputsandbox', inputsandbox)


##         share_dir = os.path.join(app.is_prepared.name,'inputsandbox')
##         for root, dirs, files in os.walk(share_dir):
##             subdir = root.replace(share_dir,'')[1:] ## [1:] removes the preceeding /
##             for f in files:
##                 inputsandbox += [File(name=os.path.join(root,f),subdir=subdir)]

        # Handle debug prep_dir e.g. copy the option pickling env to debug
        #sharedir_handler(app, 'debug', job.getDebugWorkspace().getPath())
     
        ## Here add any sandbox files/data coming from the appmasterconfig
        ## from master_configure. Catch the case where None is passed (as in tests)
        if appmasterconfig:            
            inputsandbox  += appmasterconfig.getSandboxFiles()
            outputsandbox += appmasterconfig.getOutputSandboxFiles()
   
        return self._additional_master_prepare( app,
                                                appmasterconfig,
                                                unique(inputsandbox),
                                                unique(outputsandbox) )

    def _additional_master_prepare( self,
                                    app,
                                    appmasterconfig,
                                    inputbox,
                                    outputbox ):
        
        return StandardJobConfig( inputbox=unique(inputsandbox),
                                  outputbox=unique(outputsandbox))
    

    def prepare(self,app,appsubconfig,appmasterconfig,jobmasterconfig):

##         inputsandbox=[]
##         outputsandbox=[]
        job=app.getJobObject()
        
        ## Add the job.in/outputsandbox as splitters create subjobs that are
        ## seperate Job objects and therefore have their own job.in/outputsandbox
        ## which is NOT in general copied from the master in/outputsandbox
        #inputsandbox = job.inputsandbox[:] # copied in splitter
        #outputsandbox = job.outputsandbox[:]
        inputsandbox  = []
        outputsandbox = []
        
        ## Here add any sandbox files coming from the appsubconfig
        ## currently none. masterjobconfig inputsandbox added automatically
        if appsubconfig   : inputsandbox  += appsubconfig.getSandboxFiles()

        ## Strangly NEITHER the master outputsandbox OR job.outputsandbox
        ## are added automatically.
        if jobmasterconfig: outputsandbox += jobmasterconfig.getOutputSandboxFiles()
        if appsubconfig   : outputsandbox += appsubconfig.getOutputSandboxFiles()

#        script, inputsandbox, outputsandbox = self._create_runscript_sandboxes( app,
#                                                                                appsubconfig,
#                                                                                appmasterconfig,
#                                                                                jobmasterconfig,
#                                                                                unique(inputsandbox),
#                                                                                unique(outputsandbox) )

        return self._additional_prepare( app,
                                         appsubconfig,
                                         appmasterconfig,
                                         jobmasterconfig,
                                         unique(inputsandbox),
                                         unique(outputsandbox) )#,
#                                         script )
    
##         return StandardJobConfig(FileBuffer('gaudiscript.py',script,
##                                             executable=1),
##                                  inputbox=unique(inputsandbox),
##                                  outputbox=unique(outputsandbox) )

    def _additional_prepare( self,
                             app,
                             appsubconfig,
                             appmasterconfig,
                             jobmasterconfig,
                             inputsandbox,
                             outputsandbox ):#,
#                             script ):

        script = self._create_runscript( app )

        
        return StandardJobConfig( FileBuffer('gaudiscript.py', script, executable=1),
                                  inputbox  = unique(inputsandbox ),
                                  outputbox = unique(outputsandbox) )

##         return StandardJobConfig( FileBuffer('gaudiscript.py', script, executable=1),
##                                   inputbox=unique(inputsandbox),
##                                   outputbox=unique(outputsandbox) )


    def _create_runscript( self, app ):
        job=app.getJobObject()

        config = Ganga.Utility.Config.getConfig('Gaudi')

        opts = 'options.pkl'
  
        jstr = jobid_as_string(job)
        script =  "#!/usr/bin/env python\n\nimport os,sys\n\n"
        script += 'data_output = %s\n' % job.outputdata.files
        script += 'opts = \'%s\'\n' % opts
        script += 'app = \'%s\'\n' % app.appname
        script += 'app_upper = \'%s\'\n' % app.appname.upper()
        script += 'version = \'%s\'\n' % app.version
        script += 'package = \'%s\'\n' % app.package
        script += "job_output_dir = '%s/%s/%s/outputdata'\n" % \
                  (config['DataOutput'],outputdata.location,jstr)
        script += 'cp = \'%s\'\n' % config['cp_cmd']
        script += 'mkdir = \'%s\'\n' % config['mkdir_cmd']
        script += 'platform = \'%s\'\n' % app.platform
        script += 'import os \n'   
  
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

# add lib subdir in case user supplied shared libs where copied to pwd/lib
os.environ['LD_LIBRARY_PATH'] = '.:%s/lib:%s\' %(os.getcwd(),
                                                 os.environ['LD_LIBRARY_PATH'])
                                                 
#run
sys.stdout.flush()
os.environ['PYTHONPATH'] = '%s/InstallArea/python:%s' % \\
                            (os.getcwd(), os.environ['PYTHONPATH'])
os.environ['PYTHONPATH'] = '%s/InstallArea/%s/python:%s' % \\
                            (os.getcwd(), platform,os.environ['PYTHONPATH'])

cmdline = \"\"\"gaudirun.py '
"""

        for arg in app.args:
            script += arg+' '
        script += '%s data.py\"\"\" % opts \n'

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
        cmd = 'ls -l %s' % f
        print 'DEBUG INFO: Performing \"%s\" (check stdout & stderr)' % cmd
        os.system(cmd)
        sys.stdout.flush()
    # sneaky rm
    os.system('rm -f ' + f)
"""    
        return script, inputsandbox, outputsandbox



    
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

