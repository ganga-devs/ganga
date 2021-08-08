##########################################################################
# Ganga Project. http://cern.ch/ganga
#
# $Id: Notebook.py,v 1.1 $
##########################################################################

from os import path
import inspect
import uuid

from GangaCore.GPIDev.Adapters.IPrepareApp import IPrepareApp
from GangaCore.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler
from GangaCore.GPIDev.Adapters.ApplicationRuntimeHandlers import allHandlers
from GangaCore.GPIDev.Lib.File import FileUtils
from GangaCore.GPIDev.Lib.File import FileBuffer
from GangaCore.GPIDev.Lib.File import ShareDir
from GangaCore.Core.exceptions import ApplicationPrepareError
from GangaCore.GPIDev.Schema import Schema, Version, SimpleItem
from GangaCore.Utility.logging import getLogger
from GangaCore.GPIDev.Base.Proxy import getName


logger = getLogger()

class Notebook(IPrepareApp):

    """Notebook application -- execute Jupyter notebooks.

    All cells in the notebooks given as inputfiles will be evaluated 
    and the results returned in the same notebooks.

    A simple example is

    app = Notebook()
    infiles = [LocalFile('/abc/test.ipynb')]
    outfiles = [LocalFile('test.ipynb')]
    j = Job(application=app, inputfiles=files, backend=Local())
    j.submit()

    The input can come from any GangaFile type supported and the same
    is the case for the output.

    All inputfiles matching the regular expressions (default all
    files ending in .ipynb) given are executed. Other files will
    simply be unpacked and available.

    """
    _schema = Schema(Version(1, 0), {
        'version': SimpleItem(preparable=1, defvalue=None, typelist=[None, int], doc="Version of the notebook. If None, it will be assumed that it is the latest one."),
        'timeout': SimpleItem(preparable=1, defvalue=None, typelist=[None, int], doc="Timeout in seconds for executing a notebook. If None, the default value will be taken."),
        'kernel': SimpleItem(preparable=1, defvalue='python2', doc="The kernel to use for the notebook execution. Depending on configuration, python3, Root and R might be available."),
        'regexp': SimpleItem(preparable=1, defvalue=[r'.+\.ipynb$'], typelist=["str"], sequence=1, strict_sequence=0, doc="Regular expression for the inputfiles to match for executing."),
       'is_prepared': SimpleItem(defvalue=None, strict_sequence=0, visitable=1, copyable=1, hidden=0, typelist=[None, ShareDir], protected=0, comparable=1, doc='Location of shared resources. Presence of this attribute implies the application has been prepared.'),
        'hash': SimpleItem(defvalue=None, typelist=[None, str], hidden=0, doc='MD5 hash of the string representation of applications preparable attributes')
    })
    _category = 'applications'
    _name = 'Notebook'
    _exportmethods = ['prepare', 'unprepare']

    def __init__(self):
        super(Notebook, self).__init__()

    def configure(self, masterappconfig):
        return (None, None)

    def templatelocation(self):
        """Provide name of template file with absolute path"""
        dir = path.dirname(path.abspath(inspect.getfile(inspect.currentframe())))
        return path.join(dir, 'wrapperNotebookTemplate.py.template')
                                                   
    def wrapper(self, regexp, version, timeout, kernel):
        """Write a wrapper Python script that executes the notebooks"""
        wrapperscript = FileUtils.loadScript(self.templatelocation(), '')

        wrapperscript = wrapperscript.replace('###NBFILES###', str(regexp))
        wrapperscript = wrapperscript.replace('###VERSION###', str(version))
        wrapperscript = wrapperscript.replace('###TIMEOUT###', str(timeout))
        wrapperscript = wrapperscript.replace('###KERNEL###', str(kernel))
        wrapperscript = wrapperscript.replace('###UUID###', str(uuid.uuid4()))

        logger.debug('Script to run on worker node\n' + wrapperscript)
        scriptName = "notebook_wrapper_generated.py"
        runScript = FileBuffer(scriptName, wrapperscript, executable=1)

        return runScript
    
    def unprepare(self, force=False):
        """
        Revert a Notebook application back to its unprepared state.
        """
        logger.debug('Running unprepare in Notebook app')
        if self.is_prepared is not None:
            self.decrementShareCounter(self.is_prepared)
            self.is_prepared = None
        self.hash = None

    def prepare(self, force=False):
        """
        This writes the wrapper script for the Notebook application.

        """
        if force:
            self.unprepare()
    
        if (self.is_prepared is not None):
            raise ApplicationPrepareError('%s application has already been prepared. Use prepare(force=True) to prepare again.' % getName(self))

        logger.info('Preparing %s application.' % getName(self))
        self.is_prepared = ShareDir()
        logger.info('Created shared directory: %s' % (self.is_prepared.name))
    
        # Prevent orphaned shared directories
        try:
            self.checkPreparedHasParent(self)

            script = self.wrapper(self.regexp,self.version, self.timeout, self.kernel)
            logger.debug("Creating: %s" % path.join(self.getSharedPath(), script.name))
            script.create(path.join(self.getSharedPath(), script.name))

            self.post_prepare()

        except Exception as err:
            self.unprepare()
            raise

        return 1


class NotebookRTHandler(IRuntimeHandler):
    """Empty runtime handler for notebooks"""

    def prepare(self, app, appconfig, appmasterconfig, jobmasterconfig):

        from GangaCore.GPIDev.Adapters.StandardJobConfig import StandardJobConfig
        return StandardJobConfig('python', None, ['notebook_wrapper_generated.py'], None, None, [app.is_prepared.path()])


allHandlers.add('Notebook', 'LSF', NotebookRTHandler)
allHandlers.add('Notebook', 'Local', NotebookRTHandler)
allHandlers.add('Notebook', 'PBS', NotebookRTHandler)
allHandlers.add('Notebook', 'SGE', NotebookRTHandler)
allHandlers.add('Notebook', 'Condor', NotebookRTHandler)
allHandlers.add('Notebook', 'LCG', NotebookRTHandler)
allHandlers.add('Notebook', 'gLite', NotebookRTHandler)
allHandlers.add('Notebook', 'TestSubmitter', NotebookRTHandler)
allHandlers.add('Notebook', 'Interactive', NotebookRTHandler)
allHandlers.add('Notebook', 'Batch', NotebookRTHandler)
allHandlers.add('Notebook', 'Remote', NotebookRTHandler)
allHandlers.add('Notebook', 'CREAM', NotebookRTHandler)
allHandlers.add('Notebook', 'ARC', NotebookRTHandler)
