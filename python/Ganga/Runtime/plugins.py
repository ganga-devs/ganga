import Ganga.Utility.logging
logger = Ganga.Utility.logging.getLogger()

logger.debug("Loading IApplication")
import Ganga.GPIDev.Adapters.IApplication
logger.debug("Loading IBackend")
import Ganga.GPIDev.Adapters.IBackend
logger.debug("Loading ISplitter")
import Ganga.GPIDev.Adapters.ISplitter
logger.debug("Loading IMerger")
import Ganga.GPIDev.Adapters.IMerger

logger.debug("Loading GangaList")
import Ganga.GPIDev.Lib.GangaList
logger.debug("Loading File")
import Ganga.GPIDev.Lib.File
logger.debug("Loading Job")
import Ganga.GPIDev.Lib.Job

logger.debug("Loading Mergers")
import Ganga.Lib.Mergers
logger.debug("Loading Splitters")
import Ganga.Lib.Splitters

logger.debug("Loading Executable")
import Ganga.Lib.Executable
logger.debug("Loading Root")
import Ganga.Lib.Root

logger.debug("Loading LocalHost")
import Ganga.Lib.Localhost
logger.debug("Loading LCG")
import Ganga.Lib.LCG
logger.debug("Loading Condor")
import Ganga.Lib.Condor
logger.debug("Loading Interactive")
import Ganga.Lib.Interactive
logger.debug("Loading Batch")
import Ganga.Lib.Batch
logger.debug("Loading Remote")
import Ganga.Lib.Remote

logger.debug("Loading Tasks")
import Ganga.GPIDev.Lib.Tasks


logger.debug("Loading Checkers")
import Ganga.Lib.Checkers
logger.debug("Loading Notifier")
import Ganga.Lib.Notifier

logger.debug("Finished Runtime.plugins")
