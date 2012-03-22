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
from Ganga.GPIDev.Lib.File import ShareDir
from GaudiJobConfig import *
from Ganga.GPIDev.Lib.File.FileBuffer import FileBuffer
from Ganga.Utility.Config import getConfig
from Ganga.Utility.files import expandfilename
shared_path = os.path.join(expandfilename(getConfig('Configuration')['gangadir']),'shared',getConfig('Configuration')['user'])
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
    _exportmethods = ['prepare','unprepare']

    schema = get_common_gaudi_schema()
    docstr = 'The name of the script to execute. A copy will be made ' + \
             'at submission time'
    schema['script'] = FileItem(preparable=1,sequence=1,strict_sequence=0,defvalue=[],
                                doc=docstr)
    docstr = "List of arguments for the script"
    schema['args'] =  SimpleItem(preparable=1,defvalue=[],typelist=['str'],
                                 sequence=1,doc=docstr)
    docstr = 'The name of the Gaudi application (e.g. "DaVinci", "Gauss"...)'
    schema['project'] = SimpleItem(preparable=1,defvalue=None,
                                   typelist=['str','type(None)'],
                                   doc=docstr)
    docstr = 'Data/sandbox items defined in prepare'
    schema['prep_inputbox']   = SimpleItem(preparable=1,defvalue=[],hidden=1,doc=docstr)
    docstr = 'Location of shared resources. Presence of this attribute implies'\
             'the application has been prepared.'
    schema['is_prepared'] = SimpleItem(defvalue=None,
                                       strict_sequence=0,
                                       visitable=1,
                                       copyable=1,
                                       typelist=['type(None)','str'],
                                       protected=1,
                                       doc=docstr)
    _schema = Schema(Version(1, 2), schema)                                    

    def _auto__init__(self):
        if (not self.project): self.project = 'DaVinci'
        self._init(self.project,False)

    def unprepare(self,force=False):
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared.name)
            self.is_prepared = None
            self.prep_inputbox = []

    def prepare(self,force=False):
        #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        if (self.is_prepared is not None) and (force is not True):
            raise Exception('%s application has already been prepared. Use prepare(force=True) to prepare again.'%(self._name))

        logger.info('Preparing %s application.'%(self._name))
        self.is_prepared = ShareDir()
        #self.incrementShareCounter(self.is_prepared.name)#NOT NECESSARY, DONT AUTOMATICALLY

        #/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        send_to_share = self._prepare(os.path.join(shared_path,self.is_prepared.name))
        self._check_inputs()
        self.prep_inputbox  += send_to_share[:]
        self.checkPreparedHasParent(self)
        
    def master_configure(self):
        #self._master_configure()
        #self._check_inputs()
        #self.extra.master_input_files += self.script[:]
        master_input_files=self.prep_inputbox[:]
        master_input_files += self.script[:]
        #return (None,self.extra)
        return (None,GaudiJobConfig(inputbox=master_input_files))

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
        self._check_gaudi_inputs(self.script,self.project)
        if len(self.script)==0:
            logger.warning("No script defined. Will use a default " \
                           'script which is probably not what you want.')
            self.script = [File(os.path.join(
                os.path.dirname(inspect.getsourcefile(GaudiPython)),
                'options/GaudiPythonExample.py'))]
        return

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#

# Associate the correct run-time handlers to GaudiPython for various backends.

from Ganga.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaLHCb.Lib.Gaudi.GaudiRunTimeHandler import GaudiRunTimeHandler
from GangaLHCb.Lib.DIRAC.GaudiDiracRTHandler import GaudiDiracRTHandler

for backend in ['LSF','Interactive','PBS','SGE','Local','Condor','Remote']:
    allHandlers.add('GaudiPython', backend, GaudiRunTimeHandler)
allHandlers.add('GaudiPython', 'Dirac', GaudiDiracRTHandler)

#\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\#
