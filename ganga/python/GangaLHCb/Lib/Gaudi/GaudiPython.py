#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

'''Application handler for GaudiPython applications in LHCb.'''

__author__ = 'Ulrik Egede'
__date__ = "$Date: 2008-11-13 10:02:53 $"
__revision__ = "$Revision: 1.8 $"

import os
import re
import sys
import inspect
from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Schema import *
from Ganga.Core import ApplicationConfigurationError
import Ganga.Utility.Config
from Ganga.Utility.files import expandfilename
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.File import  File
from GangaLHCb.Lib.Gaudi import GaudiExtras
from GaudiUtils import *

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiPython(IApplication):
    """The Gaudi Application handler
    
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
    ds = LHCbDataset(['LFN:foo','LFN:bar'])

    # Construct and submit job object
    j=Job(application=app,backend=Dirac(),inputdata=ds)
    j.submit()

"""
    _name = 'GaudiPython'
    _category = 'applications'
    
    schema = {}
    docstr = 'The name of the script to execute. A copy will be made ' + \
             'at submission time'
    schema['script'] = FileItem(sequence=1,strict_sequence=0,defvalue=[],
                                doc=docstr)
    docstr = 'The version of the application (like "v19r2")'
    schema['version'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],doc=docstr)
    docstr = 'The platform the application is configured for (e.g. ' + \
             '"slc4_ia32_gcc34")'
    schema['platform'] = SimpleItem(defvalue=None,
                                    typelist=['str','type(None)'],doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['project'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],hidden=1,
                                   doc=docstr)
    docstr = 'Extra options to be passed onto the SetupProject command ' + \
             'used for configuring the environment. As an example ' + \
             'setting it to \'--dev\' will give access to the DEV area. ' + \
             'For full documentation of the available options see ' + \
             'https://twiki.cern.ch/twiki/bin/view/LHCb/SetupProject'
    schema['setupProjectOptions'] = SimpleItem(defvalue='',
                                               typelist=['str','type(None)'],
                                               doc=docstr)  
    _schema = Schema(Version(1, 1), schema)                                    


    def _auto__init__(self):
        if (not self.project):
            self.project = 'DaVinci'            
        if (not self.version):
            self.version = guess_version(self.project)
        if (not self.platform):
            self.platform = get_user_platform()
        
    def master_configure(self):
        '''Configures the application'''

        self.appname = self.project
        self.shell = gaudishell_setenv(self)

        self._check_inputs()
        job=self.getJobObject()
        self.extra = GaudiExtras()
        if job.inputdata:
            self.extra.inputdata = job.inputdata
            self.extra.inputdata.datatype_string=job.inputdata.datatype_string

        self.package = available_packs(self.project)

        return (None,None)

    def configure(self,master_appconfig):
        job=self.getJobObject()
        self.dataopts = dataset_to_options_string(job.inputdata)
        return (None,None)
            
    def _check_inputs(self):
        """Checks the validity of user's entries for GaudiPython schema"""
        
        check_gaudi_inputs(self.script,self.project)

        if len(self.script)==0:
            logger.warning("No script defined. Will use a default " + \
                           'script which is probably not what you want.')
            self.script = [File(os.path.join(
                os.path.dirname(inspect.getsourcefile(GaudiPython)),
                'options/GaudiPythonExample.py'))]
        return

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

#
# Associate the correct run-time handlers to GaudiPython for various backends.
#

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.Gaudi.GaudiPythonLSFRunTimeHandler \
     import GaudiPythonLSFRunTimeHandler
from GangaLHCb.Lib.Dirac.GaudiPythonDiracRunTimeHandler \
     import GaudiPythonDiracRunTimeHandler

allHandlers.add('GaudiPython', 'LSF', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'Interactive', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'PBS', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'SGE', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'Local', GaudiPythonLSFRunTimeHandler)
allHandlers.add('GaudiPython', 'Dirac', GaudiPythonDiracRunTimeHandler)
allHandlers.add('GaudiPython', 'Condor', GaudiPythonLSFRunTimeHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
