#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
'''Application handler for GaudiPython applications in LHCb.'''
import os
from os.path import split,join
import inspect
from Ganga.GPIDev.Schema import *
import Ganga.Utility.logging
from Ganga.GPIDev.Lib.File import  File
from Francesc import *
from Ganga.Utility.util import unique

logger = Ganga.Utility.logging.getLogger()

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

class GaudiPython(Francesc):
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

    schema = get_common_gaudi_schema()
    docstr = 'The name of the script to execute. A copy will be made ' + \
             'at submission time'
    schema['script'] = FileItem(sequence=1,strict_sequence=0,defvalue=[],
                                doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['project'] = SimpleItem(defvalue=None,
                                   typelist=['str','type(None)'],
                                   doc=docstr)
    _schema = Schema(Version(1, 1), schema)                                    

    def _auto__init__(self):
        if (not self.project): self.project = 'DaVinci'
        self._init(self.project,False)
        
    def master_configure(self):
        self._master_configure()
        self._check_inputs()
        self.extra.master_input_files += self.script[:]
        return (None,self.extra)

    def configure(self,master_appconfig):
        self._configure()
        name = join('.',self.script[0].subdir,split(self.script[0].name)[-1])
        script =  "from Gaudi.Configuration import *\n"
        script += "importOptions('data.opts')\n"
        script += "execfile(\'%s\')\n" % name
        self.extra.input_buffers['gaudiPythonwrapper.py'] = script
        outsb = collect_lhcb_filelist(self.getJobObject().outputsandbox)
        self.extra.outputsandbox = unique(outsb)
        return (None,self.extra)
            
    def _check_inputs(self):
        """Checks the validity of user's entries for GaudiPython schema"""
        self._check_gaudi_inputs(self.script,self.project)
        if len(self.script)==0:
            logger.warning("No script defined. Will use a default " \
                           'script which is probably not what you want.')
            self.script = [File(os.path.join(
                os.path.dirname(inspect.getsourcefile(GaudiPython)),
                'options/GaudiPythonExample.py'))]
        return

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

bschema = GaudiPython._schema.inherit_copy()
bschema['project']._meta['protected'] = 1
bschema['project']._meta['hidden'] = 1
bschema['project']._meta['defvalue'] = 'Bender'

class Bender(GaudiPython):
    """Bender application.

    Hack to convert Bender into a Ganga application.
"""
    _name = 'Bender'
    _category = 'applications'
    _schema = bschema.inherit_copy()
    _exportmethods = ['getpack', 'make', 'cmt']
    
    def _check_inputs(self):
        """Checks the validity of user's entries for GaudiPython schema"""
        self._check_gaudi_inputs(self.script,self.project)        
        if len(self.script)==0:
            logger.warning("No script defined. Will use a default " \
                           'script which is probably not what you want.')
            self.script = [File(os.path.join(
                os.path.dirname(inspect.getsourcefile(GaudiPython)),
                'options/BenderExample.py'))]
        return

    # add the next 3 b/c of bug in exportmethods dealing w/ grandchildren
    def getpack(self,options=''):
        return super(Bender,self).getpack(options)

    def make(self,argument=''):
        return super(Bender,self).make(argument)

    def cmt(self,command):
        return super(Bender,self).cmt(command)

    for method in ['getpack','make','cmt']:
        setattr(eval(method),"__doc__", getattr(GaudiPython, method).__doc__)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Associate the correct run-time handlers to GaudiPython for various backends.

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.Gaudi.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaLHCb.Lib.Dirac.GaudiDiracRunTimeHandler \
     import GaudiDiracRunTimeHandler

for app in ['GaudiPython','Bender']:
    for backend in ['LSF','Interactive','PBS','SGE','Local','Condor']:
        allHandlers.add(app, backend, GaudiRunTimeHandler)
    allHandlers.add(app, 'Dirac', GaudiDiracRunTimeHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
