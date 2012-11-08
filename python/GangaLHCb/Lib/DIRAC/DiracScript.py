#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

import os
import string
import Ganga.Utility.Config
import Ganga.Utility.logging
from Ganga.Core import BackendError
from GangaLHCb.Lib.Splitters.SplitterUtils import DiracSplitter
from Ganga.Core.exceptions import *
logger = Ganga.Utility.logging.getLogger()
config = Ganga.Utility.Config.getConfig('LHCb')

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracExe:
    '''Handles exes to be sent to DIRAC.'''
    def __init__(self,exe,args=None,log_file=None):
        if args is None: args = []
        if log_file is None: log_file = 'Ganga_Executable.log'
        self.exe = exe
        self.args = args
        self.log_file = log_file

    def write(self):
        args_str = ''
        for arg in self.args: args_str += arg + ' '
        if args_str: args_str = args_str[:-1]
        return 'j.setExecutable("%s","%s","%s")\n' \
                   % (self.exe,args_str,self.log_file)
                  
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracInputData:
    '''Handles input data sent to DIRAC.'''
    def __init__(self,data):
        self.data = data

    def write(self):
        data = self.data.getLFNs()
        contents = ''
        if len(data) > 0:
            contents += 'j.setInputData(%s)\n' % str(data)
            if hasattr(self.data,'depth'):
                contents += 'j.setAncestorDepth(%d)\n' % self.data.depth
        return contents

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracParametricInputData:
    '''Handles input data sent to DIRAC.'''
    def __init__(self, data, filesPerJob, maxFiles, ignoremissing):
        self.data = data
        self.filesPerJob = filesPerJob
        self.maxFiles = maxFiles
        self.ignoremissing = ignoremissing

    def write(self):
        self.split_data=[]
        for dataset in DiracSplitter(self.data, self.filesPerJob, self.maxFiles, self.ignoremissing):
            self.split_data.append([f.name for f in dataset])
        if len(self.split_data) > config['MaxDiracBulkJobs']:
            raise BackendError('Dirac','Number of bulk submission jobs \'%s\' exceeds the maximum allowed \'%s\' if more are needed please modify your config. Note there is a hard limit in Dirac of currently 1000.' % (len(self.split_data),config['MaxDiracBulkJobs'] ))
        contents = ''
        if len(self.split_data) > 0:
            #bulk submission
            contents += 'j.setParametricInputData(%s)\n' % str(self.split_data)
            if hasattr(self.data,'depth'):
                contents += 'j.setAncestorDepth(%d)\n' % self.data.depth
        return contents
        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracRoot:
    '''Handles ROOT macros, PYTHON scripts and executables sent to DIRAC.'''
    def __init__(self,root_app,script,log=None):
        if log is None: log = "Ganga_Root.log"
        self.root_app = root_app
        self.script = script
        self.log = log

    def write(self):
        app = self.root_app
        args = []
        if app.args:
            if app.usepython: args = [str(arg) for arg in app.args]
            else: args = app.args
        script = self.script
        arg_str = "(\"%s\",\"%s\",%s,\"%s\")" \
                  % (app.version,script,str(args),self.log)
        if app.usepython:
            if app.script.name.split('.')[-1] != 'py':
                logger.warning('Root application has "usepython" set to True,'\
                               ' but the file does not end in ".py"')
            return 'j.setRootPythonScript%s\n' % arg_str
        else:
            return 'j.setRootMacro%s\n' % arg_str

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracApplication:
    '''Handles LHCb applications sent to DIRAC. '''
    def __init__(self,gaudi_app,script,log=None):
        self.gaudi_app = gaudi_app
        self.script = script
        if log is None:
            log = 'Ganga_%s_%s.log' % (gaudi_app._name,gaudi_app.version)
        self.log = log

    def write(self):
        app_name = self.gaudi_app.appname
        version = self.gaudi_app.version
        return 'j.setApplicationScript("%s","%s","%s",logFile="%s")\n' \
               % (app_name,version,self.script,self.log)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class DiracScript:
    '''Collects info for and writes script that creates the DIRAC job.'''

    def __init__(self):
        self.settings = None
        self.input_sandbox = None
        self.output_sandbox = None
        self.exe = None
        self.name = None
        self.job_type = None
        self.inputdata = None
        self.outputdata = None
        self.dirac_opts = None
        self.platform = None

    def write(self,file_name):
        contents = '# dirac job created by ganga \n'
        contents += 'from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob\n'
        contents += 'from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb\n'
        contents += 'j = %s\n' % self.job_type
        contents += 'dirac = DiracLHCb()\n'
        contents += '\n# default commands added by ganga\n'
        if self.name: contents += 'j.setName("%s")\n' % self.name
        if self.input_sandbox:
            contents += "j.setInputSandbox(%s)\n" % str(self.input_sandbox)
        if self.output_sandbox:
            contents += "j.setOutputSandbox(%s)\n" % str(self.output_sandbox)
        if self.exe: contents += self.exe.write()
        if self.inputdata: contents += self.inputdata.write()
        if self.outputdata and self.outputdata.files:
            contents += 'j.setOutputData(%s,OutputPath="%s",OutputSE=%s)\n' % \
                        (str(self.outputdata.files),self.outputdata.location,
                         config['DiracOutputDataSE'])
        if self.platform:
            contents += "j.setSystemConfig('%s')\n" % self.platform
        contents += '\n'
        if self.settings:            
            contents += '# <-- user settings \n'
            for key in self.settings:
                value = self.settings[key]
                if type(value) == type(''):
                    contents += 'j.set%s("%s")\n' % (key,value)
                else:
                    contents += 'j.set%s(%s)\n' % (key,str(value))
            contents += '# user settings -->\n'
        contents += '\n'
        if self.dirac_opts:
            contents += '# diracOpts added by user\n'
            contents += '%s\n' % self.dirac_opts
        contents += '\n# submit the job to dirac\n'
        contents += 'result = dirac.submit(j)\n'
        # write it out
        file = open(file_name,'w')
        file.write(contents)
        file.close()
        
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
