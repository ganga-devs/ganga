
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for Bender applications in LHCb.'''
import os
import tempfile
import pprint
import shutil
from os.path import split, join
from Ganga.GPIDev.Schema.Schema import FileItem, SimpleItem
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.File import File
from Ganga.Utility.util import unique
from Ganga.Core import ApplicationConfigurationError
from Ganga.GPIDev.Lib.File import ShareDir
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from GangaGaudi.Lib.Applications.GaudiBase import GaudiBase
from GangaGaudi.Lib.Applications.GaudiUtils import fillPackedSandbox, gzipFile
from Ganga.Utility.files import expandfilename, fullpath
from Ganga.Utility.Config import getConfig
from Ganga.Utility.Shell import Shell
from AppsBaseUtils import guess_version
from Ganga.GPIDev.Base.Proxy import isType
#
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig

# Added for XML PostProcessing
from GangaLHCb.Lib.RTHandlers.RTHUtils import getXMLSummaryScript
from GangaLHCb.Lib.Applications import XMLPostProcessor

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
    _exportmethods = GaudiBase._exportmethods[:]
    _exportmethods += ['prepare', 'unprepare']

    _schema = GaudiBase._schema.inherit_copy()
    docstr = 'The package the application belongs to (e.g. "Sim", "Phys")'
    _schema.datadict['package'] = SimpleItem(defvalue=None,
                                             typelist=['str', 'type(None)'],
                                             doc=docstr)
    docstr = 'The package where your top level requirements file is read '  \
             'from. Can be written either as a path '  \
             '\"Tutorial/Analysis/v6r0\" or in traditional notation '  \
             '\"Analysis v6r0 Tutorial\"'
    _schema.datadict['masterpackage'] = SimpleItem(defvalue=None,
                                                   typelist=[
                                                       'str', 'type(None)'],
                                                   doc=docstr)
    docstr = 'Extra options to be passed onto the SetupProject command '\
             'used for configuring the environment. As an example '\
             'setting it to \'--dev\' will give access to the DEV area. '\
             'For full documentation of the available options see '\
             'https://twiki.cern.ch/twiki/bin/view/LHCb/SetupProject'
    _schema.datadict['setupProjectOptions'] = SimpleItem(defvalue='',
                                                         typelist=[
                                                             'str', 'type(None)'],
                                                         doc=docstr)
    docstr = 'The name of the module to import. A copy will be made ' \
             'at submission time'
    _schema.datadict['module'] = FileItem(preparable=1, defvalue=File(), doc=docstr)
    docstr = 'The name of the Gaudi application (Bender)'
    _schema.datadict['project'] = SimpleItem(preparable=1, defvalue='Bender', hidden=1, protected=1,
                                             typelist=['str'], doc=docstr)
    docstr = 'The number of events '
    _schema.datadict['events'] = SimpleItem(
        defvalue=-1, typelist=['int'], doc=docstr)
    docstr = 'Parameres for module '
    _schema.datadict['params'] = SimpleItem(
        defvalue={}, typelist=['dict', 'str', 'int', 'bool', 'float'], doc=docstr)
    _schema.version.major += 2
    _schema.version.minor += 0

    #def __init__(self):
    #    super(Bender, self).__init__()

    def _get_default_version(self, gaudi_app):
        return guess_version(self, gaudi_app)

    def _auto__init__(self):
        if (not self.appname) and (not self.project):
            self.project = 'Bender'  # default
        if (not self.appname):
            self.appname = self.project
        self._init()

    def _getshell(self):

        import EnvironFunctions
        return EnvironFunctions._getshell(self)

    def prepare(self, force=False):
        super(Bender, self).prepare(force)
        self._check_inputs()

        share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),
                                 'shared',
                                 getConfig('Configuration')['user'],
                                 self.is_prepared.name)
        fillPackedSandbox([self.module],
                          os.path.join(share_dir,
                                       'inputsandbox',
                                       '_input_sandbox_%s.tar' % self.is_prepared.name))

        gzipFile(os.path.join(share_dir, 'inputsandbox', '_input_sandbox_%s.tar' % self.is_prepared.name),
                 os.path.join(
                     share_dir, 'inputsandbox', '_input_sandbox_%s.tgz' % self.is_prepared.name),
                 True)

        # add the newly created shared directory into the metadata system if
        # the app is associated with a persisted object
        self.checkPreparedHasParent(self)
        self.post_prepare()
        logger.debug("Finished Preparing Application in %s" % share_dir)

    def master_configure(self):
        return (None, StandardJobConfig())

    def configure(self, master_appconfig):

        # self._configure()
        modulename = split(self.module.name)[-1].split('.')[0]
        script = """
from copy import deepcopy
from Gaudi.Configuration import *
importOptions('data.py')
import %s as USERMODULE
EventSelectorInput = deepcopy(EventSelector().Input)
FileCatalogCatalogs = deepcopy(FileCatalog().Catalogs)
EventSelector().Input=[]
FileCatalog().Catalogs=[]\n""" % modulename

        script_configure = "USERMODULE.configure(EventSelectorInput,FileCatalogCatalogs%s)\n"
        if self.params:
            param_string = ",params=%s" % self.params
        else:
            param_string = ""

        script_configure = script_configure % param_string
        script += script_configure

        script += "USERMODULE.run(%d)\n" % self.events
        script += getXMLSummaryScript()
        # add summary.xml
        outputsandbox_temp = XMLPostProcessor._XMLJobFiles()
        outputsandbox_temp += unique(self.getJobObject().outputsandbox)
        outputsandbox = unique(outputsandbox_temp)

        input_files = []
        input_files += [FileBuffer('gaudipython-wrapper.py', script)]
        logger.debug("Returning StandardJobConfig")
        return (None, StandardJobConfig(inputbox=input_files,
                                        outputbox=outputsandbox))

    def _check_inputs(self):
        """Checks the validity of user's entries for GaudiPython schema"""
        # Always check for None OR empty
        #logger.info("self.module: %s" % str(self.module))
        if isType(self.module, str):
            self.module = File(self.module)
        if self.module.name == None:
            raise ApplicationConfigurationError(None, "Application Module not requested")
        elif self.module.name == "":
            raise ApplicationConfigurationError(None, "Application Module not requested")
        else:
            # Always check we've been given a FILE!
            self.module.name = fullpath(self.module.name)
            if not os.path.isfile(self.module.name):
                msg = 'Module file %s not found.' % self.module.name
                raise ApplicationConfigurationError(None, msg)

    def postprocess(self):
        XMLPostProcessor.postprocess(self, logger)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
# Associate the correct run-time handlers to GaudiPython for various backends.

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
from GangaLHCb.Lib.RTHandlers.LHCbGaudiDiracRunTimeHandler import LHCbGaudiDiracRunTimeHandler

for backend in ['LSF', 'Interactive', 'PBS', 'SGE', 'Local', 'Condor', 'Remote']:
    allHandlers.add('Bender', backend, LHCbGaudiRunTimeHandler)
allHandlers.add('Bender', 'Dirac', LHCbGaudiDiracRunTimeHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
