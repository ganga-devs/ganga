#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for GaudiPython applications in LHCb.'''
import os, pprint
from os.path import split,join
import inspect
from Ganga.GPIDev.Schema import *
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.File import  File
#from Francesc import *
from Ganga.Utility.util import unique
from Ganga.GPIDev.Lib.File import ShareDir
#from GaudiJobConfig import *
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
logger = Ganga.Utility.logging.getLogger()
from GangaGaudi.Lib.Applications.GaudiBase import GaudiBase
from GangaGaudi.Lib.Applications.GaudiUtils import *
from Ganga.Core import ApplicationConfigurationError
from Ganga.Utility.files import expandfilename, fullpath
from Ganga.Utility.Config import getConfig
from Ganga.Utility.Shell import Shell
from AppsBaseUtils import guess_version
import CMTscript
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
import shutil, tempfile

# Added for XML PostProcessing
from GangaLHCb.Lib.RTHandlers.RTHUtils import getXMLSummaryScript
from GangaLHCb.Lib.Applications import XMLPostProcessor

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiPython(GaudiBase):
    """The GaudiPython Application handler
    
    The GaudiPython application handler is for running LHCb GaudiPython
    jobs. This means running scripts where you are in control of the events
    loop etc. If you are usually running jobs using the gaudirun script
    this is *not* the application handler you should use. Instead use the
    DaVinci, Gauss, ... handlers.

    For its configuration it needs to know what application and version to
    use for setting up the environment. More detailed configuration options
    are described in the schema below.
    
    An example of submitting a GaudiPython job to Dirac could be:
    
    app = GaudiPython(project='DaVinci', version='v19r14')

    # Give absolute path to the python file to be executed. 
    # If several files are given the subsequent ones will go into the
    # sandbox but it is the users responsibility to include them
    app.script = ['/afs/...../myscript.py']

    # Define dataset
    ds = LHCbDataset(['LFN:spam','LFN:eggs'])

    # Construct and submit job object
    j=Job(application=app,backend=Dirac(),inputdata=ds)
    j.submit()

"""
    _name = 'GaudiPython'
    _category = 'applications'
    _exportmethods = GaudiBase._exportmethods[:]
    _exportmethods += ['prepare','unprepare']

    _schema = GaudiBase._schema.inherit_copy()
    docstr = 'The package the application belongs to (e.g. "Sim", "Phys")'
    _schema.datadict['package'] = SimpleItem(defvalue=None,
                                             typelist=['str','type(None)'],
                                             doc=docstr)
    docstr = 'The package where your top level requirements file is read '  \
             'from. Can be written either as a path '  \
             '\"Tutorial/Analysis/v6r0\" or in a CMT style notation '  \
             '\"Analysis v6r0 Tutorial\"'
    _schema.datadict['masterpackage'] = SimpleItem(defvalue=None,
                                                   typelist=['str','type(None)'],
                                                   doc=docstr)
    docstr = 'Extra options to be passed onto the SetupProject command '\
             'used for configuring the environment. As an example '\
             'setting it to \'--dev\' will give access to the DEV area. '\
             'For full documentation of the available options see '\
             'https://twiki.cern.ch/twiki/bin/view/LHCb/SetupProject'
    _schema.datadict['setupProjectOptions'] = SimpleItem(defvalue='',
                                                         typelist=['str','type(None)'],
                                                         doc=docstr)
    docstr = 'The name of the script to execute. A copy will be made ' + \
             'at submission time'
    _schema.datadict['script'] = FileItem(preparable=1,sequence=1,strict_sequence=0,defvalue=[],
                                          doc=docstr)
    docstr = "List of arguments for the script"
    _schema.datadict['args'] =  SimpleItem(defvalue=[],typelist=['str'],
                                           sequence=1,doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    _schema.datadict['project'] = SimpleItem(preparable=1,defvalue=None,
                                             typelist=['str','type(None)'],
                                             doc=docstr)
    _schema.version.major += 2
    _schema.version.minor += 0

    def _get_default_version(self, gaudi_app):
        return guess_version(gaudi_app)

    def _attribute_filter__set__(self,n,v):
        if n == 'project':
            self.appname=v
        return v

    def _auto__init__(self):
        #if (not self.project): self.project = 'DaVinci'
        if (not self.appname) and (not self.project):
            self.project = 'DaVinci' #default
            #raise ApplicationConfigurationError(None,"no appname/project")
        if (not self.appname): self.appname = self.project
        #self.appname = self.project
        self._init(False)

    def _getshell(self):
        opts = ''
        if self.setupProjectOptions: opts = self.setupProjectOptions

        fd = tempfile.NamedTemporaryFile()
        script = '#!/bin/sh\n'
        if self.user_release_area:
            script += 'User_release_area=%s; export User_release_area\n' % \
                      expandfilename(self.user_release_area)
        if self.platform:
#            script += 'export CMTCONFIG=%s\n' % self.platform
            script += '. `which LbLogin.sh` -c %s\n' % self.platform
        useflag = ''
        if self.masterpackage:
            (mpack, malg, mver) = CMTscript.parse_master_package(self.masterpackage)
            useflag = '--use \"%s %s %s\"' % (malg, mver, mpack)
        cmd = '. SetupProject.sh %s %s %s %s' % (useflag,opts,self.appname,self.version) 
        script += '%s \n' % cmd
        script += ''
        script += getXMLSummaryScript()
        fd.write(script)
        fd.flush()
        logger.debug(script)

        self.shell = Shell(setup=fd.name)
        if (not self.shell): raise ApplicationConfigurationError(None,'Shell not created.')
        
        logger.debug(pprint.pformat(self.shell.env))
        
        fd.close()
        app_ok = False
        ver_ok = False
        for var in self.shell.env:
            if var.find(self.appname) >= 0: app_ok = True
            if self.shell.env[var].find(self.version) >= 0: ver_ok = True
        if not app_ok or not ver_ok:
            msg = 'Command "%s" failed to properly setup environment.' % cmd
            logger.error(msg)
            raise ApplicationConfigurationError(None,msg)

        return self.shell.env

##         super(type(self), self)._getshell()
            
##         opts = ''
##         if self.setupProjectOptions: opts = self.setupProjectOptions
            
##         useflag = ''
##         if self.masterpackage:
##             (mpack, malg, mver) = parse_master_package(self.masterpackage)
##             useflag = '--use \"%s %s %s\"' % (malg, mver, mpack)
##         cmd = '. SetupProject.sh %s %s %s %s' % (useflag,opts,self.appname,self.version) 
##         # script += '%s \n' % cmd
##         self.shell.cmd('%s \n' % cmd)

##         app_ok = False
##         ver_ok = False
##         for var in self.shell.env:
##             if var.find(self.appname) >= 0: app_ok = True
##             if self.shell.env[var].find(self.version) >= 0: ver_ok = True
##         if not app_ok or not ver_ok:
##             msg = 'Command "%s" failed to properly setup environment.' % cmd
##             logger.error(msg)
##             raise ApplicationConfigurationError(None,msg)



    def prepare(self,force=False):
        super(GaudiPython,self).prepare(force)
        self._check_inputs()

        share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                  'shared',
                                  getConfig('Configuration')['user'],
                                  self.is_prepared.name)
##         if not os.path.isdir(share_path): os.makedirs(share_path) 
##         for f in self.script:
##             shutil.copy(expandfilename(f.name),share_path)

        fillPackedSandbox(self.script,
                          os.path.join(share_dir,
                                       'inputsandbox',
                                       '_input_sandbox_%s.tar' % self.is_prepared.name))
        gzipFile(os.path.join(share_dir,'inputsandbox','_input_sandbox_%s.tar' % self.is_prepared.name),
                 os.path.join(share_dir,'inputsandbox','_input_sandbox_%s.tgz' % self.is_prepared.name),
                 True)
        # add the newly created shared directory into the metadata system if the app is associated with a persisted object
        self.checkPreparedHasParent(self)
        self.post_prepare()

    
    def master_configure(self):
        #self._master_configure()
        #self._check_inputs()
        #self.extra.master_input_files += self.script[:]
        #master_input_files=self.prep_inputbox[:]
        #master_input_files += self.script[:]
        #return (None,self.extra)
        #return (None,GaudiJobConfig(inputbox=master_input_files))
        return (None, StandardJobConfig())

    def configure(self,master_appconfig):
        #self._configure()
        name = join('.',self.script[0].subdir,split(self.script[0].name)[-1])
        script =  "from Gaudi.Configuration import *\n"
        if self.args:
            script += 'import sys\nsys.argv += %s\n' % str(self.args)
        script += "importOptions('data.py')\n"
        script += "execfile(\'%s\')\n" % name
        #self.extra.input_buffers['gaudipython-wrapper.py'] = script

        
        #outsb = self.getJobObject().outputsandbox
        #outputsandbox = unique(self.getJobObject().outputsandbox)
        # add summary.xml
        outputsandbox_temp = XMLPostProcessor._XMLJobFiles()
        outputsandbox_temp += unique(self.getJobObject().outputsandbox)
        outputsandbox = unique(outputsandbox_temp)

        #input_dir = self.getJobObject().getInputWorkspace().getPath()
        input_files=[]
        #input_files += [FileBuffer(os.path.join(input_dir,'gaudipython-wrapper.py'),script).create()]
        input_files += [FileBuffer('gaudipython-wrapper.py',script)]
        #self.extra.input_files += [FileBuffer(os.path.join(input_dir,'gaudipython-wrapper.py'),script).create()]
        #return (None,self.extra)
        return (None,StandardJobConfig(inputbox=input_files,
                                       outputbox=outputsandbox))
            
    def _check_inputs(self):
        """Checks the validity of user's entries for GaudiPython schema"""
        #self._check_gaudi_inputs(self.script,self.project)
        if len(self.script)==0:
            logger.warning("No script defined. Will use a default " \
                           'script which is probably not what you want.')
            self.script = [File(os.path.join(
                os.path.dirname(inspect.getsourcefile(GaudiPython)),
                'options/GaudiPythonExample.py'))]
        else:
            for f in self.script: f.name = fullpath(f.name)

        return

    def postprocess(self):
        XMLPostProcessor.postprocess(self,logger)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Associate the correct run-time handlers to GaudiPython for various backends.

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
from GangaLHCb.Lib.RTHandlers.LHCbGaudiDiracRunTimeHandler import LHCbGaudiDiracRunTimeHandler

for backend in ['LSF','Interactive','PBS','SGE','Local','Condor','Remote']:
    allHandlers.add('GaudiPython', backend, LHCbGaudiRunTimeHandler)
allHandlers.add('GaudiPython', 'Dirac', LHCbGaudiDiracRunTimeHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
