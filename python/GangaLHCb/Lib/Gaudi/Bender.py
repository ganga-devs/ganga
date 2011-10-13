
#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#                                                                       
'''Application handler for GaudiPython applications in LHCb.'''
import os
from os.path import split,join
from Ganga.GPIDev.Schema import *
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.File import  File
from GangaLHCb.Lib.Gaudi.Francesc import *
from Ganga.Utility.util import unique
from Ganga.Core import ApplicationConfigurationError
from Ganga.GPIDev.Lib.File import ShareDir
from Ganga.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class Bender(Francesc):
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

    schema = get_common_gaudi_schema()
    docstr = 'The name of the module to import. A copy will be made ' \
             'at submission time'
    schema['module'] = FileItem(doc=docstr)
    docstr = 'The name of the Gaudi application (Bender)'
    schema['project'] = SimpleItem(defvalue='Bender',hidden=1,protected=1,
                                   typelist=['str'],doc=docstr)
    docstr = 'The number of events '
    schema['events'] = SimpleItem(defvalue=-1,typelist=['int'],doc=docstr)
    schema['is_prepared'] = SimpleItem(defvalue=None,
                                       strict_sequence=0,
                                       visitable=1,
                                       copyable=1,
                                       typelist=['type(None)','str'],
                                       protected=1,
                                       doc=docstr)
    _schema = Schema(Version(1, 3), schema)

    def _auto__init__(self):
        if (not self.project): self.project = 'Bender'
        self._init(self.project,False)

    def unprepare(self,force=False):
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None

    def prepare(self,force=False):
        #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        if (self.is_prepared is not None) and (force is not True):
            raise Exception('%s application has already been prepared. Use prepare(force=True) to prepare again.'%(self._name))

        logger.info('Preparing %s application.'%(self._name))
        self.is_prepared = ShareDir()
        self.incrementShareCounter(self.is_prepared.name)

        #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        send_to_share = self._prepare()
        self._check_inputs()
        
        return send_to_share
    
    def master_configure(self):
        #self._master_configure()
        #self._check_inputs()
        self.extra.master_input_files += [self.module]
        #return (None,self.extra)
        return (None,StandardJobConfig(inputbox=self.extra.master_input_files))

    def configure(self,master_appconfig):
        self._configure()
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
        outsb = self.getJobObject().outputsandbox
        self.extra.outputsandbox = unique(outsb)


        input_dir = job.getInputWorkspace().getPath()
        self.extra.input_files += [FileBuffer(os.path.join(input_dir,'gaudipython-wrapper.py'),script).create()]
        return (None,StandardJobConfig(inputbox=self.extra.input_files,
                                       outputbox=self.extra.outputsandbox))

    def _check_inputs(self):
        """Checks the validity of user's entries for GaudiPython schema"""
        if not os.path.exists(self.module.name):
            msg = 'Module file %s not found.' % self.module.name
            raise ApplicationConfigurationError(None,msg)
        self._check_gaudi_inputs([self.module],self.project)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
# Associate the correct run-time handlers to GaudiPython for various backends.
                
from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.Gaudi.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaLHCb.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler

for backend in ['LSF','Interactive','PBS','SGE','Local','Condor','Remote']:
    allHandlers.add('Bender', backend, GaudiRunTimeHandler)
allHandlers.add('Bender', 'Dirac', GaudiDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#


