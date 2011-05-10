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
# Main Compact application class
class Compact(IApplication):
    """The main Nasim executable"""

    _schema = Schema(Version(2,0), {
                 'job_file'          : SimpleItem(defvalue='',doc='The job file to run'),
                 'cmc_version'       : SimpleItem(defvalue='',doc='The version of CMC to use')
                 })
    _category = 'applications'
    _name = 'Compact'
    _exportmethods = []

