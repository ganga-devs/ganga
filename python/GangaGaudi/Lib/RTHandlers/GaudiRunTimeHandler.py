#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
from Ganga.GPIDev.Lib.Job import Job
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
import Ganga.Utility.Config
from Ganga.GPIDev.Base.Proxy import isType
from Ganga.GPIDev.Lib.File import FileBuffer, File
from Ganga.Core import TypeMismatchError
from Ganga.Utility.util import unique
import shutil
from RunTimeHandlerUtils import sharedir_handler
from Ganga.GPIDev.Lib.File.OutputFileManager import getOutputSandboxPatterns
from GangaGaudi.Lib.RTHandlers.RunTimeHandlerUtils import master_sandbox_prepare, sandbox_prepare
logger = Ganga.Utility.logging.getLogger()
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


class GaudiRunTimeHandler(IRuntimeHandler):

    """This is the application runtime handler class for Gaudi applications 
    using the local, interactive and LSF backends."""

    def master_prepare(self, app, appmasterconfig):
        inputsandbox, outputsandbox = master_sandbox_prepare(app, appmasterconfig, ['inputsandbox'])
        return StandardJobConfig(inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox))

    def prepare(self, app, appsubconfig, appmasterconfig, jobmasterconfig):

        inputsandbox, outputsandbox = sandbox_prepare(
            app, appsubconfig, appmasterconfig, jobmasterconfig)

        run_script = self.__create_run_script(app,
                                              appsubconfig,
                                              appmasterconfig,
                                              jobmasterconfig,
                                              inputsandbox,
                                              outputsandbox)
        return StandardJobConfig(FileBuffer('gaudi-script.py', run_script, executable=1),
                                 inputbox=unique(inputsandbox),
                                 outputbox=unique(outputsandbox))

    def __create_run_script(self,
                            app,
                            appsubconfig,
                            appmasterconfig,
                            jobmasterconfig,
                            inputsandbox,
                            outputsandbox):
        job = app.getJobObject()

        opts = 'options.pkl'

        script = "#!/usr/bin/env python\n\nimport os,sys\n\n"
        script += 'opts = \'%s\'\n' % opts
        script += 'app = \'%s\'\n' % app.appname
        script += 'app_upper = \'%s\'\n' % app.appname.upper()
        script += 'version = \'%s\'\n' % app.version
        script += 'package = \'%s\'\n' % app.package
        script += 'platform = \'%s\'\n' % app.platform

        if opts:
            script += """# check that options file exists
if not os.path.exists(opts):
    opts = 'notavailable'
    os.environ['JOBOPTPATH'] = opts
else:
    os.environ['JOBOPTPATH'] = '%s/%s/%s_%s/%s/%s/%s/options/job.opts' \
                               % (os.environ[app + '_release_area'],app_upper,
                                  app_upper,version,package,app,version)
    import sys
    sys.stdout.write('Using the master optionsfile: '+ str(opts))
    sys.stdout.flush()
    
"""

        script += """# check that SetupProject.sh script exists, then execute it

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
            script += arg + ' '
        script += '%s data.py\"\"\" % opts \n'

        script += """
# run command
os.system(cmdline)
"""
        return script, inputsandbox, outputsandbox


#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
