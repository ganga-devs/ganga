
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#                                                                       
'''Application handler for Bender applications in LHCb.'''
import os, tempfile,pprint, shutil
from os.path import split,join
from Ganga.GPIDev.Schema import *
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.File import  File
#from GangaLHCb.Lib.Gaudi.Francesc import *
from Ganga.Utility.util import unique
from Ganga.Core import ApplicationConfigurationError
from Ganga.GPIDev.Lib.File import ShareDir
#from GaudiJobConfig import *
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from GangaGaudi.Lib.Applications.GaudiBase import GaudiBase
from Ganga.Utility.files import expandfilename, fullpath
from Ganga.Utility.Config import getConfig
from Ganga.Utility.Shell import Shell
from AppsBaseUtils import guess_version
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Bender(GaudiBase):
    """The Bender application handler

    The user specifies a module file (via Bender.module) which contains a
    Bender python module and the number of events they want to run on
    (via Bender.events).  The user's module is then run on the data by
    calling:

    USERMODULE.configure(EventSelectorInput,FileCatalogCatalogs)
    USERMODULE.run(NUMEVENTS)
    """
    
    _name = 'Bender'
    _category = 'applications'
    _exportmethods = ['prepare','unprepare']

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
    docstr = 'The name of the module to import. A copy will be made ' \
             'at submission time'
    _schema.datadict['module'] = FileItem(preparable=1,doc=docstr)
    docstr = 'The name of the Gaudi application (Bender)'
    _schema.datadict['project'] = SimpleItem(preparable=1,defvalue='Bender',hidden=1,protected=1,
                                   typelist=['str'],doc=docstr)
    docstr = 'The number of events '
    _schema.datadict['events'] = SimpleItem(preparable=1,defvalue=-1,typelist=['int'],doc=docstr)
##     _schema.datadict['is_prepared'] = SimpleItem(defvalue=None,
##                                                  strict_sequence=0,
##                                                  visitable=1,
##                                                  copyable=1,
##                                                  typelist=['type(None)','str'],
##                                                  protected=1,
##                                                  doc=docstr)

    def _get_default_version(self, gaudi_app):
        return guess_version(gaudi_app)

    def _auto__init__(self):
        #if (not self.project): self.project = 'Bender'
        if (not self.appname) and (not self.project):
            self.project = 'Bender' #default
            #raise ApplicationConfigurationError(None,"no appname/project")
        if (not self.appname): self.appname = self.project
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
            script += 'export CMTCONFIG=%s\n' % self.platform
        useflag = ''
        if self.masterpackage:
            (mpack, malg, mver) = parse_master_package(self.masterpackage)
            useflag = '--use \"%s %s %s\"' % (malg, mver, mpack)
        cmd = '. SetupProject.sh %s %s %s %s' % (useflag,opts,self.appname,self.version) 
        script += '%s \n' % cmd
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
        super(Bender,self).prepare(force)
        self._check_inputs()

        share_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                  'shared',
                                  getConfig('Configuration')['user'],
                                  self.is_prepared.name,
                                  'inputsandbox')
        if not os.path.isdir(share_path): os.makedirs(share_path) 
        shutil.copy(expandfilename(self.module.name),share_path)

        # add the newly created shared directory into the metadata system if the app is associated with a persisted object
        self.checkPreparedHasParent(self)
        self.post_prepare()


    def master_configure(self):
        #self._master_configure()
        #self._check_inputs()
##         master_input_files=self.prep_inputbox[:]
##         master_input_files += [self.module]
        #self.extra.master_input_files += [self.module]
        #return (None,self.extra)
        return (None,StandardJobConfig())

    def configure(self,master_appconfig):
        #self._configure()
        modulename = split(self.module.name)[-1].split('.')[0]
        script =  "from Gaudi.Configuration import *\n"
        script += "importOptions('data.py')\n"
        script += "import %s as USERMODULE\n" % modulename
        script += "EventSelectorInput = EventSelector().Input\n"
        script += "FileCatalogCatalogs = FileCatalog().Catalogs\n"
        script += \
               "USERMODULE.configure(EventSelectorInput,FileCatalogCatalogs)\n"
        script += "USERMODULE.run(%d)\n" % self.events
        #self.extra.input_buffers['gaudipython-wrapper.py'] = script
        #outsb = self.getJobObject().outputsandbox
        outputsandbox = unique(self.getJobObject().outputsandbox)


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
        self.module.name = fullpath(self.module.name)
        if not os.path.exists(self.module.name):
            msg = 'Module file %s not found.' % self.module.name
            raise ApplicationConfigurationError(None,msg)
        #self._check_gaudi_inputs([self.module],self.project)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
# Associate the correct run-time handlers to GaudiPython for various backends.
                
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
from GangaLHCb.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler

for backend in ['LSF','Interactive','PBS','SGE','Local','Condor','Remote']:
    allHandlers.add('Bender', backend, LHCbGaudiRunTimeHandler)
allHandlers.add('Bender', 'Dirac', GaudiDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


