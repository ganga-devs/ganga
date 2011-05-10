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
# Main Nasim application class
class Nasim(IApplication):
    """The main Nasim executable"""

    _schema = Schema(Version(2,0), {
                 'job_file'          : FileItem(doc='The job file to run'),
                 'titles_file'       : FileItem(doc='The user titles file to use'),
                 'beam'              : SimpleItem(defvalue=1, doc='Beam type to use (1 = k+, 2 = k-)'),
                 'seed'              : SimpleItem(defvalue=-1, doc='Random seed to use (-1 == based on time)'),
                 'num_triggers'      : SimpleItem(defvalue=10, doc='Number of triggers'),
                 'run_number'        : SimpleItem(defvalue=15000, doc='Run Number'),
                 'prod_num'          : SimpleItem(defvalue=0, doc='Production Number (default == 0)')
                 })
    _category = 'applications'
    _name = 'Nasim'
    _exportmethods = []
    
    def configure(self,masterappconfig):
        logger.debug('Nasim configure called')
        return (None,None)
    
    def master_configure(self):
        logger.debug('Athena master_configure called')

        job = self.getJobObject()

        # check the job file exists
        if not self.job_file.exists():
            raise ApplicationConfigurationError(None,'The job option file %s does not exist.' % self.job_file.name)

        # check the user titles file exists
        if not self.titles_file.exists():
            raise ApplicationConfigurationError(None,'The user titles file %s does not exist.' % self.titles_file.name)

        # check for valid run number
        if not self.run_number > 15000:
            raise ApplicationConfigurationError(None,'Run number smaller than 15000')

        # check for valid output dataset
        if not job.outputdata._name == 'NA48OutputDataset':
            raise ApplicationConfigurationError(None, "Incorrect output dataset. You must specify an 'NA48OutputDataset'")

            
        return (0, None)
