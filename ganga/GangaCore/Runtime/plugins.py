import GangaCore.Lib.Notifier
import GangaCore.Lib.Checkers
import GangaCore.GPIDev.Lib.Tasks
import GangaCore.Lib.Remote
import GangaCore.Lib.Batch
import GangaCore.Lib.Interactive
import GangaCore.Lib.Condor
import GangaCore.Lib.LCG
import GangaCore.Lib.Localhost
import GangaCore.Lib.Notebook
import GangaCore.Lib.Root
import GangaCore.Lib.Executable
import GangaCore.Lib.Splitters
import GangaCore.Lib.Mergers
import GangaCore.GPIDev.Lib.Job
import GangaCore.GPIDev.Lib.File
import GangaCore.GPIDev.Lib.GangaList
import GangaCore.GPIDev.Adapters.IMerger
import GangaCore.GPIDev.Adapters.ISplitter
import GangaCore.GPIDev.Adapters.IBackend
import GangaCore.GPIDev.Adapters.IApplication
import GangaCore.Utility.logging
logger = GangaCore.Utility.logging.getLogger()

logger.debug("Loading IApplication")
logger.debug("Loading IBackend")
logger.debug("Loading ISplitter")
logger.debug("Loading IMerger")

logger.debug("Loading GangaList")
logger.debug("Loading File")
logger.debug("Loading Job")

logger.debug("Loading Mergers")
logger.debug("Loading Splitters")

logger.debug("Loading Executable")
logger.debug("Loading Root")
logger.debug("Loading Notebook")

logger.debug("Loading LocalHost")
logger.debug("Loading LCG")
logger.debug("Loading Condor")
logger.debug("Loading Interactive")
logger.debug("Loading Batch")
logger.debug("Loading Remote")

logger.debug("Loading Tasks")


logger.debug("Loading Checkers")
logger.debug("Loading Notifier")

logger.debug("Finished Runtime.plugins")
