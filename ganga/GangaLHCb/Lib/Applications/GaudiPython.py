#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for GaudiPython applications in LHCb.'''
import os
import pprint
from os.path import split, join
import inspect
from GangaCore.GPIDev.Schema import FileItem, SimpleItem
import GangaCore.Utility.logging
from GangaCore.GPIDev.Lib.File import File
from GangaCore.Utility.util import unique
from GangaCore.GPIDev.Lib.File import ShareDir
from GangaCore.GPIDev.Lib.File.FileBuffer import FileBuffer
from GangaGaudi.Lib.Applications.GaudiBase import GaudiBase
from GangaGaudi.Lib.Applications.GaudiUtils import fillPackedSandbox, gzipFile
from GangaCore.Core.exceptions import ApplicationConfigurationError
from GangaCore.Utility.files import expandfilename, fullpath
from GangaCore.Utility.Config import getConfig
from GangaCore.Utility.Shell import Shell
from .AppsBaseUtils import guess_version
from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
import shutil
import tempfile

# Added for XML PostProcessing
from GangaLHCb.Lib.RTHandlers.RTHUtils import getXMLSummaryScript
from GangaLHCb.Lib.Applications import XMLPostProcessor

logger = GangaCore.Utility.logging.getLogger()

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
    docstr = 'The name of the script to execute. A copy will be made ' + \
             'at submission time'
    _schema.datadict['script'] = FileItem(preparable=1, sequence=1, strict_sequence=0, defvalue=[],
                                          doc=docstr)
    docstr = "List of arguments for the script"
    _schema.datadict['args'] = SimpleItem(defvalue=[], typelist=['str'],
                                          sequence=1, doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    _schema.datadict['project'] = SimpleItem(preparable=1, defvalue=None,
                                             typelist=['str', 'type(None)'],
                                             doc=docstr)
    _schema.version.major += 2
    _schema.version.minor += 0

    def _get_default_version(self, gaudi_app):
        return guess_version(self, gaudi_app)

    def _attribute_filter__set__(self, n, v):
        if n == 'project':
            self.appname = v
        return v

    def _auto__init__(self):
        if (not self.appname) and (not self.project):
            self.project = 'DaVinci'  # default
        if (not self.appname):
            self.appname = self.project
        self._init()

    def _getshell(self):
        from . import EnvironFunctions
        return EnvironFunctions._getshell(self)

    def prepare(self, force=False):
        super(GaudiPython, self).prepare(force)
        self._check_inputs()

        share_dir = os.path.join(expandfilename(getConfig('Configuration')['gangadir']), 'shared', getConfig('Configuration')['user'], self.is_prepared.name)

        fillPackedSandbox(self.script, os.path.join(share_dir, 'inputsandbox', '_input_sandbox_%s.tar' % self.is_prepared.name))
        gzipFile(os.path.join(share_dir, 'inputsandbox', '_input_sandbox_%s.tar' % self.is_prepared.name),
                 os.path.join(share_dir, 'inputsandbox', '_input_sandbox_%s.tgz' % self.is_prepared.name), True)
        # add the newly created shared directory into the metadata system if
        # the app is associated with a persisted object
        self.checkPreparedHasParent(self)
        self.post_prepare()

    def master_configure(self):
        return (None, StandardJobConfig())

    def configure(self, master_appconfig):
        # self._configure()
        name = join('.', self.script[0].subdir, split(self.script[0].name)[-1])
        script = "from Gaudi.Configuration import *\n"
        if self.args:
            script += 'import sys\nsys.argv += %s\n' % str(self.args)
        script += "importOptions('data.py')\n"
        script += "execfile(\'%s\')\n" % name

        # add summary.xml
        outputsandbox_temp = XMLPostProcessor._XMLJobFiles()
        outputsandbox_temp += unique(self.getJobObject().outputsandbox)
        outputsandbox = unique(outputsandbox_temp)

        input_files = []
        input_files += [FileBuffer('gaudipython-wrapper.py', script)]
        logger.debug("Returning Job Configuration")
        return (None, StandardJobConfig(inputbox=input_files,
                                        outputbox=outputsandbox))

    def _check_inputs(self):
        """Checks the validity of user's entries for GaudiPython schema"""
        if len(self.script) == 0:
            logger.warning("No script defined. Will use a default "
                           'script which is probably not what you want.')
            self.script = [File(os.path.join(
                os.path.dirname(inspect.getsourcefile(GaudiPython)),
                'options/GaudiPythonExample.py'))]
        else:
            for f in self.script:
                f.name = fullpath(f.name)

        return

    def postprocess(self):
        XMLPostProcessor.postprocess(self, logger)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Associate the correct run-time handlers to GaudiPython for various backends.

from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.RTHandlers.LHCbGaudiRunTimeHandler import LHCbGaudiRunTimeHandler
from GangaLHCb.Lib.RTHandlers.LHCbGaudiDiracRunTimeHandler import LHCbGaudiDiracRunTimeHandler

for backend in ['LSF', 'Interactive', 'PBS', 'SGE', 'Local', 'Condor', 'Remote']:
    allHandlers.add('GaudiPython', backend, LHCbGaudiRunTimeHandler)
allHandlers.add('GaudiPython', 'Dirac', LHCbGaudiDiracRunTimeHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
