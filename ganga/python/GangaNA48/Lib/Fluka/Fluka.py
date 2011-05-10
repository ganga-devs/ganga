from Ganga.Core.exceptions import ApplicationConfigurationError
from Ganga.GPIDev.Base import GangaObject
from Ganga.GPIDev.Schema import *

from Ganga.Utility.Config import makeConfig, ConfigError
from Ganga.Utility.logging import getLogger
from Ganga.Utility.files import expandfilename

from Ganga.GPIDev.Adapters.IApplication import IApplication
from Ganga.GPIDev.Adapters.IRuntimeHandler import IRuntimeHandler

from Ganga.Lib.Mergers.Merger import *

# ------------------------------------------------------
# Main Fluka application class
class Fluka(IApplication):
    """The main Fluka executable"""

    _schema = Schema(Version(2,0), {
                 'card_file'          : FileItem(doc='The card file to use'),
                 'seed'              : SimpleItem(defvalue=1, doc='Random seed to use'),
                 'num_triggers'      : SimpleItem(defvalue=10, doc='Number of triggers')
                 })
    _category = 'applications'
    _name = 'Fluka'
    _exportmethods = []
    
    def configure(self,masterappconfig):
        logger.debug('Fluka configure called')
        return (None,None)
    
    def master_configure(self):
        logger.debug('Fluka master_configure called')

        job = self.getJobObject()

        # check the job file exists
        if not self.card_file.exists():
            raise ApplicationConfigurationError(None,'The card file %s does not exist.' % self.card_file.name)

        # check for valid output dataset
        if not job.outputdata._name == 'NA48OutputDataset':
            raise ApplicationConfigurationError(None, "Incorrect output dataset. You must specify an 'NA48OutputDataset'")
            
        return (0, None)
